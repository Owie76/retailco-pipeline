"""
RetailCo dlt Pipeline
---------------------
Reads raw data from the lake PostgreSQL database (raw schema)
and loads it into the warehouse PostgreSQL database (raw schema).

Features:
- Incremental loading (only moves new/updated rows)
- Type coercion between source and destination
- Idempotent (running twice produces identical results)
- Handles all 9 entities
"""

import os
import logging
from datetime import datetime, timezone
from typing import Iterator

import dlt
from dlt.sources import DltResource
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ─── Logging Setup ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ─── Load Environment Variables ───
load_dotenv()

# ─── Lake Database Config ───
LAKE_DB_CONFIG = {
    "host":     os.getenv("LAKE_DB_HOST", "localhost"),
    "port":     int(os.getenv("LAKE_DB_PORT", 5433)),
    "dbname":   os.getenv("LAKE_DB_NAME", "lake"),
    "user":     os.getenv("LAKE_DB_USER", "postgres"),
    "password": os.getenv("LAKE_DB_PASSWORD", "postgres"),
}

# ─── Warehouse Database Config ───
WAREHOUSE_DB_CONFIG = {
    "host":     os.getenv("WAREHOUSE_DB_HOST", "localhost"),
    "port":     int(os.getenv("WAREHOUSE_DB_PORT", 5434)),
    "dbname":   os.getenv("WAREHOUSE_DB_NAME", "warehouse"),
    "user":     os.getenv("WAREHOUSE_DB_USER", "postgres"),
    "password": os.getenv("WAREHOUSE_DB_PASSWORD", "postgres"),
}

# ─── Entities to Load ───
# Each entry: (table_name, primary_key, cursor_field)
ENTITIES = [
    ("stores",               "id", "updated_at"),
    ("employees",            "id", "updated_at"),
    ("payment_methods",      "id", "updated_at"),
    ("customers",            "id", "updated_at"),
    ("products",             "id", "updated_at"),
    ("orders",               "id", "updated_at"),
    ("order_items",          "id", "updated_at"),
    ("payments",             "id", "updated_at"),
    ("inventory_movements",  "id", "updated_at"),
]


# ──────────────────────────────────────────────────────────────────────────────
# LAKE CONNECTION
# ──────────────────────────────────────────────────────────────────────────────
def get_lake_connection():
    """Returns a connection to the lake PostgreSQL database."""
    return psycopg2.connect(**LAKE_DB_CONFIG)


# ──────────────────────────────────────────────────────────────────────────────
# SOURCE FUNCTION — Reads from Lake
# ──────────────────────────────────────────────────────────────────────────────
def read_entity_from_lake(
    conn,
    table_name: str,
    last_loaded_at: str = None
) -> Iterator[dict]:
    """
    Reads records from the lake database for a given entity.
    If last_loaded_at is provided, only reads records updated after that time.
    Flattens the JSONB raw_data column into individual fields.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

        if last_loaded_at:
            logger.info(f"Incremental load for {table_name} from {last_loaded_at}")
            cur.execute(
                f"""
                SELECT
                    id,
                    raw_data,
                    loaded_at,
                    updated_at
                FROM raw.{table_name}
                WHERE updated_at > %s
                ORDER BY updated_at ASC
                """,
                (last_loaded_at,)
            )
        else:
            logger.info(f"Full load for {table_name}")
            cur.execute(
                f"""
                SELECT
                    id,
                    raw_data,
                    loaded_at,
                    updated_at
                FROM raw.{table_name}
                ORDER BY updated_at ASC
                """
            )

        rows = cur.fetchall()
        logger.info(f"{table_name}: {len(rows)} rows to load")

        for row in rows:
            # Flatten raw_data JSONB into the record
            record = dict(row["raw_data"]) if row["raw_data"] else {}

            # Add metadata columns
            record["_lake_id"]         = str(row["id"])
            record["_lake_loaded_at"]  = row["loaded_at"].isoformat() if row["loaded_at"] else None
            record["_lake_updated_at"] = row["updated_at"].isoformat() if row["updated_at"] else None

            # Type coercions — ensure correct types
            record = coerce_types(record, table_name)

            yield record


# ──────────────────────────────────────────────────────────────────────────────
# TYPE COERCION
# ──────────────────────────────────────────────────────────────────────────────
def coerce_types(record: dict, table_name: str) -> dict:
    """
    Ensures correct data types for known fields.
    Converts strings to appropriate types where needed.
    """
    # Boolean fields
    bool_fields = ["isDeleted", "is_deleted"]
    for field in bool_fields:
        if field in record and record[field] is not None:
            if isinstance(record[field], str):
                record[field] = record[field].lower() == "true"

    # Decimal fields
    decimal_fields = [
        "unitPrice", "costPrice", "unit_price", "cost_price",
        "amountPaid", "amount_paid", "discountAmount", "discount_amount",
        "totalAmount", "total_amount", "quantity"
    ]
    for field in decimal_fields:
        if field in record and record[field] is not None:
            try:
                record[field] = float(record[field])
            except (ValueError, TypeError):
                record[field] = None

    # Integer fields
    int_fields = ["quantity", "stockQuantity", "stock_quantity"]
    for field in int_fields:
        if field in record and record[field] is not None:
            try:
                record[field] = int(record[field])
            except (ValueError, TypeError):
                record[field] = None

    # Timestamp fields — ensure ISO format strings
    timestamp_fields = [
        "createdAt", "updatedAt", "effectiveFrom", "registeredAt",
        "created_at", "updated_at", "effective_from", "registered_at",
        "orderedAt", "paidAt", "shippedAt", "deliveredAt"
    ]
    for field in timestamp_fields:
        if field in record and record[field] is not None:
            if isinstance(record[field], datetime):
                record[field] = record[field].isoformat()

    return record


# ──────────────────────────────────────────────────────────────────────────────
# dlt PIPELINE
# ──────────────────────────────────────────────────────────────────────────────
def run_pipeline():
    """
    Main dlt pipeline function.
    Reads from lake and loads into warehouse for all 9 entities.
    """
    logger.info("========== RetailCo dlt Pipeline Started ==========")

    # ── Connect to lake ───
    lake_conn = get_lake_connection()

    # ── Configure dlt pipeline ───
    pipeline = dlt.pipeline(
        pipeline_name="retailco_warehouse",
        destination=dlt.destinations.postgres(
            f"postgresql://{WAREHOUSE_DB_CONFIG['user']}:"
            f"{WAREHOUSE_DB_CONFIG['password']}@"
            f"{WAREHOUSE_DB_CONFIG['host']}:"
            f"{WAREHOUSE_DB_CONFIG['port']}/"
            f"{WAREHOUSE_DB_CONFIG['dbname']}"
        ),
        dataset_name="raw",
    )

    try:
        for table_name, pk_field, cursor_field in ENTITIES:
            logger.info(f"--- Loading entity: {table_name} ---")

            # ── Get last loaded timestamp from dlt state ───
            last_loaded_at = None
            try:
                with pipeline.sql_client() as client:
                    with client.execute_query(
                        f"SELECT MAX({cursor_field}) FROM raw.{table_name}"
                    ) as cursor:
                        row = cursor.fetchone()
                        if row and row[0]:
                            last_loaded_at = row[0]
                            logger.info(
                                f"Last loaded timestamp for "
                                f"{table_name}: {last_loaded_at}"
                            )
            except Exception:
                # Table doesn't exist yet — first run
                logger.info(
                    f"No existing data for {table_name} — doing full load."
                )

            # ── Read from lake ───
            records = list(read_entity_from_lake(
                lake_conn,
                table_name,
                last_loaded_at
            ))

            if not records:
                logger.info(f"No new records for {table_name}. Skipping.")
                continue

            # ── Load into warehouse ───
            load_info = pipeline.run(
                records,
                table_name=table_name,
                primary_key="_lake_id",
                write_disposition="merge",
            )

            logger.info(f"Loaded {len(records)} records into {table_name}")
            logger.info(f"Load info: {load_info}")

    finally:
        lake_conn.close()
        logger.info("Lake connection closed.")

    logger.info("========== RetailCo dlt Pipeline Completed ==========")


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_pipeline()