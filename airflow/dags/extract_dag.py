"""
RetailCo Extract DAG
--------------------
Runs the ERP extractor daily.
Extracts all 9 entities from the ERP REST API
and loads raw data into the lake PostgreSQL database.

Schedule: Daily at midnight
Retries: 3 attempts with exponential backoff
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ─── Default Arguments ───
default_args = {
    "owner":            "retailco",
    "depends_on_past":  False,
    "start_date":       datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay":  timedelta(minutes=10),
}

# ─── DAG Definition ───
with DAG(
    dag_id="retailco_extract",
    default_args=default_args,
    description="Extract all 9 entities from ERP API into lake PostgreSQL",
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["retailco", "extract", "checkpoint2"],
) as dag:

    # ─── Task 1: Health Check ───
    # Verify the ERP API is up before attempting extraction
    health_check = BashOperator(
        task_id="erp_api_health_check",
        bash_command="""
            response=$(curl -s -o /dev/null -w "%{http_code}" \
                -H "X-API-Key: $ERP_API_KEY" \
                $ERP_BASE_URL/health)
            if [ "$response" != "200" ]; then
                echo "ERP API health check failed with status $response"
                exit 1
            fi
            echo "ERP API is healthy. Status: $response"
        """,
    )

    # ─── Task 2: Install Dependencies ───
    # Ensure all Python dependencies are installed
    install_deps = BashOperator(
        task_id="install_dependencies",
        bash_command="pip install -r /opt/airflow/extractor/requirements.txt -q",
    )

    # ─── Task 3: Run Extraction ───
    # Run the main extractor script
    run_extraction = BashOperator(
        task_id="run_extraction",
        bash_command="python /opt/airflow/extractor/extract.py",
        execution_timeout=timedelta(hours=3),
    )

    # ─── Task 4: Verify Row Counts ───
    # Quick sanity check — confirm data landed in the lake
    verify_counts = BashOperator(
        task_id="verify_row_counts",
        bash_command="""
            python -c "
import psycopg2
import os

conn = psycopg2.connect(
    host=os.getenv('LAKE_DB_HOST'),
    port=os.getenv('LAKE_DB_PORT'),
    dbname=os.getenv('LAKE_DB_NAME'),
    user=os.getenv('LAKE_DB_USER'),
    password=os.getenv('LAKE_DB_PASSWORD')
)

entities = [
    'stores', 'employees', 'payment_methods', 'customers',
    'products', 'orders', 'order_items', 'payments',
    'inventory_movements'
]

with conn.cursor() as cur:
    for entity in entities:
        cur.execute(f'SELECT COUNT(*) FROM raw.{entity}')
        count = cur.fetchone()[0]
        print(f'{entity}: {count} rows')
        if count == 0:
            raise Exception(f'FAILED: {entity} has 0 rows after extraction')

conn.close()
print('All entity row counts verified successfully.')
"
        """,
    )

    # ─── Task Dependencies ───
    # health_check → install_deps → run_extraction → verify_counts
    health_check >> install_deps >> run_extraction >> verify_counts