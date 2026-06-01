"""
RetailCo ERP Extractor
----------------------
Extracts all 9 entities from the ERP REST API and loads them
into the lake PostgreSQL database (raw schema).

Features:
- Full extract on first run, incremental on subsequent runs
- Cursor-based pagination
- Retry with exponential backoff on 500/timeout
- Rate limit handling (429 + Retry-After)
- Idempotent upserts (no duplicate primary keys)
- Watermark storage per entity
"""

import os
import time
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ─── Logging Setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ─── Load Environment Variables ───────────────────────────────────────────────
load_dotenv()

API_KEY = os.getenv("ERP_API_KEY")
BASE_URL = os.getenv("ERP_BASE_URL", "https://hngstage8da-55c7f5f769c8.herokuapp.com")

LAKE_DB_CONFIG = {
    "host":     os.getenv("LAKE_DB_HOST", "localhost"),
    "port":     int(os.getenv("LAKE_DB_PORT", 5433)),
    "dbname":   os.getenv("LAKE_DB_NAME", "lake"),
    "user":     os.getenv("LAKE_DB_USER", "postgres"),
    "password": os.getenv("LAKE_DB_PASSWORD", "postgres"),
}

# ─── Entities to Extract ──────────────────────────────────────────────────────
# Each entry: (endpoint, table_name, primary_key_field)
ENTITIES = [
    ("stores",               "stores",               "id"),
    ("employees",            "employees",             "id"),
    ("payment_methods",      "payment_methods",       "id"),
    ("customers",            "customers",             "id"),
    ("products",             "products",              "id"),
    ("orders",               "orders",                "id"),
    ("order_items",          "order_items",           "id"),
    ("payments",             "payments",              "id"),
    ("inventory_movements",  "inventory_movements",   "id"),
]

# ─── Retry Configuration ──────────────────────────────────────────────────────
MAX_RETRIES = 5
INITIAL_BACKOFF = 1   # seconds
MAX_BACKOFF = 60      # seconds


# ──────────────────────────────────────────────────────────────────────────────
# API CALL WITH RETRY + BACKOFF
# ──────────────────────────────────────────────────────────────────────────────
def api_get(endpoint: str, params: dict) -> dict:
    """
    Makes a GET request to the ERP API.
    Handles:
    - 429 Rate Limit: waits for Retry-After seconds then retries
    - 500 / timeout: retries with exponential backoff up to MAX_RETRIES
    """
    url = f"{BASE_URL}/{endpoint}/"
    headers = {"X-API-Key": API_KEY}
    backoff = INITIAL_BACKOFF

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"GET {url} | params={params} | attempt={attempt}")
            response = requests.get(url, headers=headers, params=params, timeout=30)

            # ── Rate limited ──────────────────────────────────────────────────
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", backoff))
                logger.warning(f"Rate limited. Waiting {retry_after}s before retry.")
                time.sleep(retry_after)
                continue

            # ── Transient server error ────────────────────────────────────────
            if response.status_code == 500:
                logger.warning(f"500 error. Backing off {backoff}s before retry.")
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue

            # ── Success ───────────────────────────────────────────────────────
            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            logger.warning(f"Request timed out. Backing off {backoff}s before retry.")
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)

    raise Exception(f"Failed to fetch {url} after {MAX_RETRIES} attempts.")


# ──────────────────────────────────────────────────────────────────────────────
# PAGINATION — Fetch All Pages for an Entity
# ──────────────────────────────────────────────────────────────────────────────
def fetch_all_pages(endpoint: str, updated_after: Optional[str] = None) -> list:
    """
    Fetches all pages of data for a given endpoint.
    Follows cursors until has_more is False.
    Passes updated_after for incremental loads.
    """
    all_records = []
    params = {"limit": 50}

    if updated_after:
        params["updated_after"] = updated_after
        logger.info(f"Incremental extract for {endpoint} from {updated_after}")
    else:
        logger.info(f"Full extract for {endpoint}")

    while True:
        response = api_get(endpoint, params)

        records = response.get("data", [])
        all_records.extend(records)

        meta = response.get("meta", {})
        has_more = meta.get("has_more", False)
        cursor = meta.get("cursor")

        logger.info(f"{endpoint}: fetched {len(records)} records | has_more={has_more}")

        if not has_more or not cursor:
            break

        # Pass cursor for next page
        params["cursor"] = cursor

    logger.info(f"{endpoint}: total records fetched = {len(all_records)}")
    return all_records


# ──────────────────────────────────────────────────────────────────────────────
# DATABASE — Get Connection
# ──────────────────────────────────────────────────────────────────────────────
def get_db_connection():
    """Returns a connection to the lake PostgreSQL database."""
    return psycopg2.connect(**LAKE_DB_CONFIG)


# ──────────────────────────────────────────────────────────────────────────────
# DATABASE — Setup Raw Schema and Tables
# ──────────────────────────────────────────────────────────────────────────────
def setup_raw_schema(conn):
    """
    Creates the raw schema and all entity tables if they don't exist.
    Also creates the watermark table.
    """
    with conn.cursor() as cur:

        # Create raw schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        # Watermark table — stores last successful updated_at per entity
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.watermarks (
                entity      VARCHAR(100) PRIMARY KEY,
                last_updated_at TIMESTAMP WITH TIME ZONE,
                last_run_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)

        # One table per entity — stores raw JSON as JSONB + metadata
        entities = [
            "stores", "employees", "payment_methods", "customers",
            "products", "orders", "order_items", "payments",
            "inventory_movements"
        ]

        for entity in entities:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS raw.{entity} (
                    id          VARCHAR(100) PRIMARY KEY,
                    raw_data    JSONB        NOT NULL,
                    loaded_at   TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at  TIMESTAMP WITH TIME ZONE
                );
            """)

        conn.commit()
        logger.info("Raw schema and tables created successfully.")


# ──────────────────────────────────────────────────────────────────────────────
# DATABASE — Upsert Records (Idempotent)
# ──────────────────────────────────────────────────────────────────────────────
def upsert_records(conn, table_name: str, records: list, pk_field: str):
    """
    Upserts records into the raw schema table.
    On conflict (same primary key), updates the raw_data and updated_at.
    This makes the extractor idempotent — running it twice produces
    identical results with no duplicate rows.
    """
    if not records:
        logger.info(f"No records to upsert for {table_name}.")
        return

    rows = []
    for record in records:
        record_id = str(record.get(pk_field, ""))
        raw_data = json.dumps(record)
        updated_at = record.get("updatedAt") or record.get("updated_at")
        rows.append((record_id, raw_data, updated_at))

    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO raw.{table_name} (id, raw_data, updated_at)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                raw_data   = EXCLUDED.raw_data,
                updated_at = EXCLUDED.updated_at,
                loaded_at  = NOW();
            """,
            rows
        )
        conn.commit()

    logger.info(f"Upserted {len(rows)} records into raw.{table_name}.")


# ──────────────────────────────────────────────────────────────────────────────
# WATERMARK — Get Last Updated At
# ──────────────────────────────────────────────────────────────────────────────
def get_watermark(conn, entity: str) -> Optional[str]:
    """
    Returns the last successful updated_at timestamp for an entity.
    Returns None if this is the first run (full extract needed).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_updated_at FROM raw.watermarks WHERE entity = %s;",
            (entity,)
        )
        row = cur.fetchone()
        if row and row[0]:
            return row[0].isoformat()
        return None


# ──────────────────────────────────────────────────────────────────────────────
# WATERMARK — Save Last Updated At
# ──────────────────────────────────────────────────────────────────────────────
def save_watermark(conn, entity: str, records: list):
    """
    Saves the maximum updatedAt from the fetched records as the new watermark.
    This ensures the next incremental run only fetches newer records.
    """
    if not records:
        return

    # Find the maximum updatedAt across all fetched records
    timestamps = []
    for r in records:
        ts = r.get("updatedAt") or r.get("updated_at")
        if ts:
            timestamps.append(ts)

    if not timestamps:
        return

    max_timestamp = max(timestamps)

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw.watermarks (entity, last_updated_at, last_run_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (entity) DO UPDATE SET
                last_updated_at = EXCLUDED.last_updated_at,
                last_run_at     = NOW();
        """, (entity, max_timestamp))
        conn.commit()

    logger.info(f"Watermark saved for {entity}: {max_timestamp}")


# ──────────────────────────────────────────────────────────────────────────────
# MAIN EXTRACT FUNCTION
# ──────────────────────────────────────────────────────────────────────────────
def extract_entity(conn, endpoint: str, table_name: str, pk_field: str):
    """
    Full pipeline for a single entity:
    1. Get watermark (last updated_at)
    2. Fetch all pages from API (incremental or full)
    3. Upsert records into lake
    4. Save new watermark
    """
    logger.info(f"--- Starting extraction for: {endpoint} ---")

    # Step 1 — Get watermark
    updated_after = get_watermark(conn, entity=table_name)

    # Step 2 — Fetch all pages
    records = fetch_all_pages(endpoint, updated_after=updated_after)

    if not records:
        logger.info(f"No new records for {endpoint}. Skipping upsert.")
        return

    # Step 3 — Upsert into lake
    upsert_records(conn, table_name, records, pk_field)

    # Step 4 — Save watermark
    save_watermark(conn, table_name, records)

    logger.info(f"--- Completed extraction for: {endpoint} ---")


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────
def run_extraction():
    """
    Main entry point. Connects to the lake DB, sets up schema,
    then extracts all 9 entities in sequence.
    """
    logger.info("========== RetailCo ERP Extraction Started ==========")

    # Connect to lake database
    conn = get_db_connection()

    try:
        # Set up raw schema and tables
        setup_raw_schema(conn)

        # Extract all entities
        for endpoint, table_name, pk_field in ENTITIES:
            try:
                extract_entity(conn, endpoint, table_name, pk_field)
            except Exception as e:
                logger.error(f"Failed to extract {endpoint}: {e}")
                raise

    finally:
        conn.close()
        logger.info("Database connection closed.")

    logger.info("========== RetailCo ERP Extraction Completed ==========")


if __name__ == "__main__":
    run_extraction()