import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.db import get_connection


# ─── DEFAULT ARGS ─────────────────────────────────────────

default_args = {
    "owner":             "pipeline_user",
    "retries":           3,
    "retry_delay":       timedelta(minutes=5),
    "email_on_failure":  False,
    "depends_on_past":   False,
}


# ─── HELPERS ──────────────────────────────────────────────

def load_active_locations(source_id=1):
    """Load all active locations from config at runtime"""
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT
                l.id,
                l.name,
                l.region
            FROM config.source_locations sl
            JOIN config.locations l ON l.id = sl.location_id
            WHERE sl.source_id = %s
              AND sl.is_active  = TRUE
              AND l.is_active   = TRUE
            ORDER BY l.name
        """, (source_id,))
        return [
            {"location_id": row[0], "name": row[1], "region": row[2]}
            for row in cur.fetchall()
        ]
    finally:
        cur.close()
        conn.close()


def ingest_province(location_id, location_name, source_id=1, **context):
    from pipeline.extractor import APIExtractor
    from pipeline.validator import WeatherValidator
    from pipeline.loader    import PostgreSQLLoader
    from utils.logger       import Logger

    logger = Logger(f"Ingest_{location_name.replace(' ', '_')}")

    try:
        # ── Extract — one province only
        extractor = APIExtractor(source_id=source_id, logger=logger)

        # filter to this province only
        extractor.active_locations = [
            loc for loc in extractor.active_locations
            if loc["location_id"] == location_id
        ]

        if not extractor.active_locations:
            logger.warning(f"No active config found for location_id={location_id}")
            return

        rows = extractor.extract()

        # ── Validate
        validator  = WeatherValidator(logger=logger)
        valid_rows, result = validator.validate(rows)

        if not valid_rows:
            logger.warning(
                f"{location_name} — all rows rejected "
                f"({result.rejection_rate()}% rejection rate)"
            )
            return

        # ── Load
        loader   = PostgreSQLLoader(logger=logger)
        inserted = loader.load(valid_rows)

        logger.success(
            f"{location_name} — inserted {inserted} row(s) "
            f"into bronze.weather"
        )

        # push metadata to XCom for health check task
        return {
            "location":    location_name,
            "extracted":   result.total_rows,
            "rejected":    result.rejected_rows,
            "inserted":    inserted,
        }

    except Exception as e:
        logger.error(f"{location_name} — ingest failed: {e}")
        raise


def run_health_check(**context):
    ti       = context["ti"]
    task_ids = context["task_ids"]

    results       = []
    failed_locs   = []
    total_inserted = 0

    for task_id in task_ids:
        meta = ti.xcom_pull(task_ids=task_id)
        if meta:
            results.append(meta)
            total_inserted += meta.get("inserted", 0)
        else:
            failed_locs.append(task_id)

    total_tasks  = len(task_ids)
    failed_count = len(failed_locs)
    success_rate = round(
        (total_tasks - failed_count) / total_tasks * 100, 1
    )

    print(f"\n── DAG 1 Health Check ──────────────────────────")
    print(f"  provinces total    : {total_tasks}")
    print(f"  provinces success  : {total_tasks - failed_count}")
    print(f"  provinces failed   : {failed_count}")
    print(f"  total rows inserted: {total_inserted}")
    print(f"  success rate       : {success_rate}%")

    if failed_locs:
        print(f"  failed             : {', '.join(failed_locs)}")

    # fail the DAG if more than 20% of provinces failed
    if success_rate < 80:
        raise ValueError(
            f"Too many provinces failed: "
            f"{failed_count}/{total_tasks} "
            f"({100 - success_rate}% failure rate)"
        )

    print(f"  status             : ✔ healthy\n")


# ─── DAG ──────────────────────────────────────────────────

with DAG(
    dag_id            = "weather_ingest",
    description       = "Hourly weather ingestion — 25 provinces → bronze.weather",
    default_args      = default_args,
    start_date        = datetime(2026, 7, 1),
    schedule_interval = "@hourly",
    catchup           = False,
    tags              = ["weather", "ingest", "bronze"],
    max_active_runs   = 1,       # only one run at a time
) as dag:

    # load locations at DAG parse time
    locations = load_active_locations(source_id=1)

    # ── dynamically create one task per province
    province_tasks = []
    for loc in locations:
        task = PythonOperator(
            task_id         = f"ingest_{loc['name'].lower().replace(' ', '_')}",
            python_callable = ingest_province,
            op_kwargs       = {
                "location_id":   loc["location_id"],
                "location_name": loc["name"],
                "source_id":     1,
            },
            pool            = "weather_ingest_pool",  # max 10 at a time
            pool_slots      = 1,
        )
        province_tasks.append(task)

    # ── health check runs after all province tasks
    health_check = PythonOperator(
        task_id         = "health_check",
        python_callable = run_health_check,
        op_kwargs       = {
            "task_ids": [t.task_id for t in province_tasks]
        },
        provide_context = True,
        trigger_rule    = "all_done",  # runs even if some provinces failed
    )

    # ── set dependencies
    province_tasks >> health_check