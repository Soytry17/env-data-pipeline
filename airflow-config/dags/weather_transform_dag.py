import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DBT_PROJECT_DIR = "/opt/airflow/project/dbt_project"

task_dbt_run = BashOperator(
    task_id      = "dbt_run",
    bash_command = (
        f"cd {DBT_PROJECT_DIR} && "
        f"dbt run "
        f"--profiles-dir {DBT_PROJECT_DIR} "   # ← point to project folder
        f"--target dev"
    ),
)

task_dbt_test = BashOperator(
    task_id      = "dbt_test",
    bash_command = (
        f"cd {DBT_PROJECT_DIR} && "
        f"dbt test "
        f"--profiles-dir {DBT_PROJECT_DIR} "   # ← same
        f"--target dev"
    ),
)
default_args = {
    "owner":            "pipeline_user",
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}


def check_bronze_freshness(**context):
    """
    Verifies bronze has new data before running dbt.
    Skips transform if no new rows since last midnight.
    """
    from utils.db import get_connection
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT COUNT(*)
            FROM bronze.weather
            WHERE ingested_at >= NOW() - INTERVAL '25 hours'
        """)
        count = cur.fetchone()[0]
        print(f"New rows in last 25 hours: {count}")
        if count == 0:
            raise ValueError(
                "No new bronze data in last 25 hours — "
                "check DAG 1 weather_ingest"
            )
        print(f"✔ Bronze is fresh — proceeding with dbt build")
        return count
    finally:
        cur.close()
        conn.close()


def verify_gold_tables(**context):
    """
    Verifies Gold tables were rebuilt correctly after dbt run.
    """
    from utils.db import get_connection
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT
                'agg_daily_weather'  AS table_name,
                COUNT(*)             AS rows,
                MAX(dbt_updated_at)  AS last_updated
            FROM gold.agg_daily_weather
            UNION ALL
            SELECT
                'weather_features',
                COUNT(*),
                MAX(dbt_updated_at)
            FROM gold.weather_features
        """)
        rows = cur.fetchall()
        print(f"\n── Gold Table Verification ─────────────────")
        for row in rows:
            print(
                f"  {row[0]:<25} "
                f"rows: {row[1]:>10,}  "
                f"updated: {row[2]}"
            )
        print(f"{'─'*50}\n")
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id            = "weather_transform",
    description       = "Midnight dbt build — silver + gold layers",
    default_args      = default_args,
    start_date        = datetime(2026, 7, 1),
    schedule_interval = "0 0 * * *",    # midnight every day
    catchup           = False,
    tags              = ["weather", "dbt", "silver", "gold"],
    max_active_runs   = 1,
) as dag:

    # ── Task 1: check bronze has fresh data
    task_check_bronze = PythonOperator(
        task_id         = "check_bronze_freshness",
        python_callable = check_bronze_freshness,
        provide_context = True,
    )

    # ── Task 2: dbt run — builds silver + gold
    task_dbt_run = BashOperator(
        task_id      = "dbt_run",
        bash_command = (
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir ~/.dbt"
        ),
    )

    # ── Task 3: dbt test — validates all models
    task_dbt_test = BashOperator(
        task_id      = "dbt_test",
        bash_command = (
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir ~/.dbt"
        ),
    )

    # ── Task 4: verify gold tables rebuilt correctly
    task_verify_gold = PythonOperator(
        task_id         = "verify_gold_tables",
        python_callable = verify_gold_tables,
        provide_context = True,
    )

    # ── dependencies
    task_check_bronze >> task_dbt_run >> task_dbt_test >> task_verify_gold