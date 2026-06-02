# RetailCo Data Pipeline

A modern end-to-end data pipeline for RetailCo, a Nigerian retail chain with stores in Lagos, Abuja, Port Harcourt, and Kano. Built with Apache Airflow, PostgreSQL, dbt, dlt, and Docker.

---

## Table of Contents

- [Architecture](#architecture)
- [Design Artifacts](#design-artifacts)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Running the Pipeline](#running-the-pipeline)
- [Querying the Warehouse](#querying-the-warehouse)
- [Project Structure](#project-structure)
- [Data Quality](#data-quality)
- [Tools and Versions](#tools-and-versions)

---

## Architecture

The pipeline follows a modern data stack architecture with four layers:
ERP REST API → Lake (PostgreSQL) → Warehouse (PostgreSQL) → Marts (dbt)

All services are containerised with Docker and orchestrated by Apache Airflow.

---

## Design Artifacts

### Kimball Bus Matrix

| | dim_date | dim_customer | dim_product | dim_store | dim_employee | dim_payment_method |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **fct_sales** | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ |
| **fct_payments** | ✓ | ✓ | ✗ | ✓ | ✗ | ✓ |
| **fct_inventory_daily** | ✓ | ✗ | ✓ | ✓ | ✗ | ✗ |
| **fct_order_lifecycle** | ✓*4 | ✓ | ✗ | ✓ | ✓ | ✗ |

> **\* fct_order_lifecycle** has four separate foreign keys to `dim_date`:
> `ordered_date_key`, `paid_date_key`, `shipped_date_key`, `delivered_date_key`

### Warehouse ERD

![Warehouse ERD](docs/erd.png)

### Architecture Diagram

![Architecture Diagram](docs/architecture_diagram.png)

---

## Prerequisites

Make sure you have the following installed:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Git](https://git-scm.com)
- A valid ERP API key

---

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR-USERNAME/retailco-pipeline.git
cd retailco-pipeline
```

### 2. Create the .env File

Create a `.env` file in the root of the project:

```bash
touch .env
```

Add the following content:
ERP API
ERP_API_KEY=(your_actual_api_key_here)
ERP_BASE_URL=https://hngstage8da-55c7f5f769c8.herokuapp.com

Lake Database
LAKE_DB_HOST=lake_db
LAKE_DB_PORT=5432
LAKE_DB_NAME=lake
LAKE_DB_USER=postgres
LAKE_DB_PASSWORD=postgres

Warehouse Database
WAREHOUSE_DB_HOST=warehouse_db
WAREHOUSE_DB_PORT=5432
WAREHOUSE_DB_NAME=warehouse
WAREHOUSE_DB_USER=postgres
WAREHOUSE_DB_PASSWORD=postgres

### 3. Start All Services

```bash
docker compose up -d
```

This starts:
- PostgreSQL Lake database (port 5433)
- PostgreSQL Warehouse database (port 5434)
- Apache Airflow (port 8080)

### 4. Verify All Containers Are Running

```bash
docker ps
```

You should see 5 containers running:
- `retailco_lake`
- `retailco_warehouse`
- `retailco_airflow_db`
- `retailco_airflow_webserver`
- `retailco_airflow_scheduler`

### 5. Access the Airflow UI

Open your browser and go to: http://localhost:8080
Login with:
- **Username:** `admin`
- **Password:** `admin`

---

## Running the Pipeline

### Automatic Daily Run

The pipeline runs automatically every day at midnight via the `retailco_pipeline` DAG.

### Manual Trigger

1. Go to the Airflow UI at `http://localhost:8080`
2. Find the `retailco_pipeline` DAG
3. Click the toggle to activate it (turn it blue)
4. Click the play button ▶
5. Select **"Trigger DAG"**

### Pipeline Task Order
erp_api_health_check
↓
install_dependencies
↓
extract
↓
load
↓
dbt_snapshot
↓
dbt_staging
↓
dbt_marts
↓
dbt_test
↓
verify_pipeline

### Backfilling

To run the pipeline for a historical date range:

```bash
docker exec -it retailco_airflow_scheduler bash -c "airflow dags backfill retailco_pipeline --start-date 2026-01-01 --end-date 2026-01-31"
```

---

## Querying the Warehouse

Connect to the warehouse database:

```bash
docker exec -it retailco_warehouse psql -U postgres -d warehouse
```

### Revenue by Store

```sql
SELECT
    s.store_name,
    SUM(f.gross_amount) as total_revenue
FROM raw_marts.fct_sales f
JOIN raw_marts.dim_store s ON f.store_key = s.store_key
GROUP BY s.store_name
ORDER BY total_revenue DESC;
```

### Top 10 Products by Revenue

```sql
SELECT
    p.product_name,
    p.category,
    SUM(f.gross_amount) as total_revenue,
    SUM(f.quantity) as units_sold
FROM raw_marts.fct_sales f
JOIN raw_marts.dim_product p ON f.product_key = p.product_key
WHERE p.is_current = true
GROUP BY p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10;
```

### Revenue by Month

```sql
SELECT
    d.year,
    d.month_name,
    SUM(f.gross_amount) as total_revenue
FROM raw_marts.fct_sales f
JOIN raw_marts.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```

### Customer Segments

```sql
SELECT
    c.segment,
    COUNT(DISTINCT f.customer_key) as customer_count,
    SUM(f.gross_amount) as total_revenue,
    AVG(f.gross_amount) as avg_order_value
FROM raw_marts.fct_sales f
JOIN raw_marts.dim_customer c ON f.customer_key = c.customer_key
WHERE c.is_current = true
GROUP BY c.segment
ORDER BY total_revenue DESC;
```

### Payment Method Breakdown

```sql
SELECT
    pm.method_name,
    COUNT(*) as transaction_count,
    SUM(f.amount_paid) as total_amount
FROM raw_marts.fct_payments f
JOIN raw_marts.dim_payment_method pm
    ON f.payment_method_key = pm.payment_method_key
GROUP BY pm.method_name
ORDER BY total_amount DESC;
```

### Flagged Anomalous Payments

```sql
SELECT
    flag_reason,
    COUNT(*) as count,
    SUM(amount_paid) as total_amount
FROM raw_marts.flagged_payments
GROUP BY flag_reason
ORDER BY count DESC;
```

### Order Lifecycle Metrics

```sql
SELECT
    current_status,
    COUNT(*) as order_count,
    AVG(days_to_pay) as avg_days_to_pay,
    AVG(days_to_ship) as avg_days_to_ship,
    AVG(days_to_deliver) as avg_days_to_deliver
FROM raw_marts.fct_order_lifecycle
GROUP BY current_status
ORDER BY order_count DESC;
```

---

## Project Structure

    retailco-pipeline/
    ├── airflow/
    │   └── dags/
    │       ├── extract_dag.py
    │       ├── load_dag.py
    │       └── retailco_pipeline.py
    ├── dbt/
    │   ├── models/
    │   │   ├── staging/
    │   │   └── marts/
    │   │       ├── dimensions/
    │   │       └── facts/
    │   ├── snapshots/
    │   ├── seeds/
    │   ├── dbt_project.yml
    │   └── profiles.yml
    ├── dlt/
    │   └── pipeline.py
    ├── extractor/
    │   └── extract.py
    ├── docs/
    │   ├── bus_matrix.xlsx
    │   ├── erd.png
    │   └── architecture_diagram.png
    ├── docker-compose.yml
    ├── .env
    ├── .gitignore
    └── README.md
---

## Data Quality

| Issue | Handling |
|-------|---------|
| Soft deletes | `is_deleted` flag preserved in dimensions |
| SCD2 changes | Full history tracked in `dim_customer` and `dim_product` |
| Refunds | `is_refund` flag on `fct_payments`, negative amounts preserved |
| Anomalous payments | Isolated into `flagged_payments` table |
| Late-arriving data | Idempotent upserts handle re-processing |
| Rate limiting | Exponential backoff on 429 responses |
| Transient errors | Up to 5 retries with backoff on 500/timeout |

---

## Tools and Versions

| Tool | Version |
|------|---------|
| Apache Airflow | 2.9.1 |
| PostgreSQL | 15 |
| dbt-core | 1.11.11 |
| dbt-postgres | 1.10.0 |
| dlt | 1.27.2 |
| Python | 3.12 |
| Docker Compose | v2.40.3 |