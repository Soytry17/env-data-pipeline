import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner":            "pipeline_user",
    "retries":          1,
    "retry_delay":      timedelta(minutes=30),
    "email_on_failure": False,
}


def check_training_data(**context):
    from utils.db import get_connection
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT
                COUNT(*)                        AS total_rows,
                COUNT(DISTINCT location_id)     AS provinces,
                MIN(feature_date)               AS earliest,
                MAX(feature_date)               AS latest,
                COUNT(
                    CASE WHEN target_next_day_temp
                    IS NOT NULL THEN 1 END
                )                               AS rows_with_target
            FROM gold.weather_features
        """)
        row = cur.fetchone()
        print(f"\n── Training Data Check ─────────────────────")
        print(f"  total rows      : {row[0]:,}")
        print(f"  provinces       : {row[1]}")
        print(f"  date range      : {row[2]} → {row[3]}")
        print(f"  rows with target: {row[4]:,}")
        print(f"{'─'*50}\n")

        # need at least 100,000 rows with targets to train
        if row[4] < 100000:
            raise ValueError(
                f"Not enough training data: "
                f"{row[4]:,} rows with target "
                f"(need at least 100,000)"
            )

        # push stats to XCom for next task
        return {
            "total_rows":       row[0],
            "provinces":        row[1],
            "earliest":         str(row[2]),
            "latest":           str(row[3]),
            "rows_with_target": row[4],
        }
    finally:
        cur.close()
        conn.close()


def trigger_retraining(**context):
    """
    Placeholder for AI model retraining.
    Replace with your actual ML training code when ready.
    """
    ti    = context["ti"]
    stats = ti.xcom_pull(task_ids="check_training_data")

    print(f"\n── AI Model Retraining ─────────────────────")
    print(f"  training rows   : {stats['total_rows']:,}")
    print(f"  provinces       : {stats['provinces']}")
    print(f"  date range      : {stats['earliest']} → {stats['latest']}")
    print(f"\n  [placeholder] ML training will run here")
    print(f"  Source table    : gold.weather_features")
    print(f"  Target columns  :")
    print(f"    - target_next_day_temp")
    print(f"    - target_next_day_precip")
    print(f"    - target_will_rain_tomorrow")
    print(f"\n  ✔ Retraining trigger complete\n")

    # when you build the ML pipeline, replace above with:
    # from ml.trainer import WeatherModelTrainer
    # trainer = WeatherModelTrainer()
    # trainer.train(source_table="gold.weather_features")
    # trainer.evaluate()
    # trainer.save_model(version=context["ds"])


def log_retrain_run(**context):
    """
    Logs the retraining run to a pipeline_runs table.
    Useful for tracking model versions over time.
    """
    from utils.db import get_connection
    conn = get_connection()
    cur  = conn.cursor()
    try:
        # create table if it doesn't exist yet
        cur.execute("""
            CREATE TABLE IF NOT EXISTS config.model_retrain_log (
                id            SERIAL PRIMARY KEY,
                dag_run_id    VARCHAR(200),
                triggered_at  TIMESTAMP DEFAULT NOW(),
                status        VARCHAR(20),
                notes         TEXT
            );
        """)
        cur.execute("""
            INSERT INTO config.model_retrain_log
                (dag_run_id, status, notes)
            VALUES (%s, %s, %s)
        """, (
            context["run_id"],
            "completed",
            f"Quarterly retrain triggered by Airflow"
        ))
        conn.commit()
        print(f"✔ Retrain run logged to config.model_retrain_log")
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id            = "weather_ai_retrain",
    description       = "Quarterly AI model retraining on gold.weather_features",
    default_args      = default_args,
    start_date        = datetime(2026, 1, 1),
    schedule_interval = "0 0 1 */3 *",   # midnight on 1st of Jan, Apr, Jul, Oct
    catchup           = False,
    tags              = ["weather", "ai", "ml", "retrain"],
    max_active_runs   = 1,
) as dag:

    task_check_data = PythonOperator(
        task_id         = "check_training_data",
        python_callable = check_training_data,
        provide_context = True,
    )

    task_retrain = PythonOperator(
        task_id         = "trigger_retraining",
        python_callable = trigger_retraining,
        provide_context = True,
    )

    task_log = PythonOperator(
        task_id         = "log_retrain_run",
        python_callable = log_retrain_run,
        provide_context = True,
    )

    task_check_data >> task_retrain >> task_log