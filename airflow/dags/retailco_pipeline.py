"""
RetailCo Master Pipeline DAG
-----------------------------
End-to-end orchestration of the RetailCo data pipeline.

Task order:
    Extract -> Load -> dbt snapshot -> dbt staging -> dbt marts -> dbt test

Schedule: Daily at midnight
Retries: 3 attempts with exponential backoff
Backfill: Enabled (catchup=True)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "retailco",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="retailco_pipeline",
    default_args=default_args,
    description="End-to-end RetailCo data pipeline",
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["retailco", "pipeline", "checkpoint5"],
) as dag:

    health_check = BashOperator(
        task_id="erp_api_health_check",
        bash_command="response=$(curl -s -o /dev/null -w '%{http_code}' -H 'X-API-Key: '\"$ERP_API_KEY\"'' $ERP_BASE_URL/health) && echo \"Status: $response\"",
    )

    install_deps = BashOperator(
        task_id="install_dependencies",
        bash_command="pip install -q requests==2.31.0 psycopg2-binary==2.9.9 python-dotenv==1.0.0 'dlt[postgres]==1.27.2' dbt-core dbt-postgres",
    )

    extract = BashOperator(
        task_id="extract",
        bash_command="python /opt/airflow/extractor/extract.py",
        execution_timeout=timedelta(hours=3),
    )

    load = BashOperator(
        task_id="load",
        bash_command="python /opt/airflow/dlt/pipeline.py",
        execution_timeout=timedelta(hours=2),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/dbt && dbt snapshot --profiles-dir /opt/airflow/dbt",
        execution_timeout=timedelta(minutes=30),
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command="cd /opt/airflow/dbt && dbt run --select models/staging --profiles-dir /opt/airflow/dbt",
        execution_timeout=timedelta(minutes=30),
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command="cd /opt/airflow/dbt && dbt run --select models/marts --profiles-dir /opt/airflow/dbt",
        execution_timeout=timedelta(hours=1),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt",
        execution_timeout=timedelta(minutes=30),
    )

    verify = BashOperator(
        task_id="verify_pipeline",
        bash_command="python -c \"import psycopg2, os; conn = psycopg2.connect(host=os.getenv('WAREHOUSE_DB_HOST'), port=os.getenv('WAREHOUSE_DB_PORT'), dbname=os.getenv('WAREHOUSE_DB_NAME'), user=os.getenv('WAREHOUSE_DB_USER'), password=os.getenv('WAREHOUSE_DB_PASSWORD')); cur = conn.cursor(); cur.execute('SELECT COUNT(*) FROM raw_marts.fct_sales'); print('fct_sales:', cur.fetchone()[0]); conn.close()\"",
    )

    health_check >> install_deps >> extract >> load >> dbt_snapshot >> dbt_staging >> dbt_marts >> dbt_test >> verify