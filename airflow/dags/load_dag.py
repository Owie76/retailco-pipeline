"""
RetailCo Load DAG
-----------------
Runs the dlt pipeline daily after extraction completes.
Reads from lake PostgreSQL and loads into warehouse PostgreSQL.

Schedule: Daily at midnight
Retries: 3 attempts with exponential backoff
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

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
    dag_id="retailco_load",
    default_args=default_args,
    description="Load data from lake to warehouse using dlt",
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["retailco", "load", "checkpoint3"],
) as dag:

    # ─── Task 1: Wait for Extract DAG to Complete ───
    wait_for_extract = ExternalTaskSensor(
        task_id="wait_for_extract",
        external_dag_id="retailco_extract",
        external_task_id="verify_row_counts",
        timeout=60 * 60 * 4,        # wait up to 4 hours
        poke_interval=60,            # check every 60 seconds
        mode="poke",
    )

    # ─── Task 2: Install Dependencies ───
    install_deps = BashOperator(
        task_id="install_dependencies",
        bash_command="pip install dlt[postgres]==1.27.2 psycopg2-binary==2.9.9 python-dotenv==1.0.0 -q",
    )

    # ─── Task 3: Run dlt Pipeline ───
    run_dlt_pipeline = BashOperator(
        task_id="run_dlt_pipeline",
        bash_command="python /opt/airflow/dlt/pipeline.py",
        execution_timeout=timedelta(hours=2),
    )

    # ─── Task 4: Verify Warehouse Row Counts ───
    verify_warehouse = BashOperator(
        task_id="verify_warehouse_counts",
        bash_command="""
            python -c "
import psycopg2
import os

conn = psycopg2.connect(
    host=os.getenv('WAREHOUSE_DB_HOST'),
    port=os.getenv('WAREHOUSE_DB_PORT'),
    dbname=os.getenv('WAREHOUSE_DB_NAME'),
    user=os.getenv('WAREHOUSE_DB_USER'),
    password=os.getenv('WAREHOUSE_DB_PASSWORD')
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
            raise Exception(f'FAILED: {entity} has 0 rows in warehouse')

conn.close()
print('All warehouse row counts verified successfully.')
"
        """,
    )

    # ─── Task Dependencies ───
    # wait_for_extract → install_deps → run_dlt_pipeline → verify_warehouse
    wait_for_extract >> install_deps >> run_dlt_pipeline >> verify_warehouse