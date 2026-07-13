import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.backfill_historical import (
    load_active_locations,
    fetch_year,
    insert_batch,
    get_last_backfilled_date,
)
from utils.db     import get_connection
from utils.schema import get_or_create_partition
from utils.logger import Logger
from datetime     import date

logger = Logger("BackfillTest")

# load locations
locations = load_active_locations(source_id=1)
phnom_penh = next(l for l in locations if l["name"] == "Phnom Penh")

logger.info(f"Testing backfill for: {phnom_penh['name']}")

# fetch just 2024
year_start = date(2024, 1, 1)
year_end   = date(2024, 12, 31)

rows = fetch_year(phnom_penh, source_id=1, year_start=year_start, year_end=year_end)
logger.info(f"Fetched {len(rows)} rows for 2024")

# insert into bronze
conn = get_connection()
cur  = conn.cursor()
try:
    get_or_create_partition(cur, 2024)
    conn.commit()

    inserted = insert_batch(rows, cur)
    conn.commit()
    logger.success(f"Inserted {inserted} rows into bronze.weather_2024")

    # verify
    cur.execute("""
        SELECT
            COUNT(*)            AS total,
            MIN(observation_at) AS earliest,
            MAX(observation_at) AS latest
        FROM bronze.weather
        WHERE location_id = %s
          AND observation_at >= '2024-01-01'
    """, (phnom_penh["location_id"],))
    row = cur.fetchone()
    print(f"\n── Verification ───────────────────────────────")
    print(f"  rows     : {row[0]:,}")
    print(f"  earliest : {row[1]}")
    print(f"  latest   : {row[2]}")

    # check JSONB unpacks correctly in silver
    cur.execute("""
        SELECT
            observation_at,
            raw_data->'current'->>'temperature_2m'      AS temp,
            raw_data->'current'->>'relative_humidity_2m' AS humidity,
            raw_data->>'data_type'                       AS data_type
        FROM bronze.weather
        WHERE location_id = %s
          AND observation_at >= '2024-01-01'
        ORDER BY observation_at
        LIMIT 5
    """, (phnom_penh["location_id"],))
    sample = cur.fetchall()
    print(f"\n── Sample rows ────────────────────────────────")
    for s in sample:
        print(f"  {s[0]}  temp={s[1]}°C  humidity={s[2]}%  type={s[3]}")

finally:
    cur.close()
    conn.close()