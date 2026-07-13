import sys
import os
import json
import time
import requests
from datetime import datetime, date, timedelta

from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db     import get_connection
from utils.schema import get_or_create_partition
from utils.logger import Logger



logger = Logger("HistoricalBackfill")


# ─── CONFIG

HISTORICAL_API_URL = "https://archive-api.open-meteo.com/v1/archive"

HOURLY_PARAMS = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "precipitation",
    "weather_code",
    "surface_pressure",
    "cloud_cover",
    "visibility",
]

START_DATE     = date(2020, 1, 1)    # backfill from year 2020
MAX_WORKERS    = 3                   # parallel province fetches
BATCH_DAYS     = 365                 # fetch one year at a time per province
REQUEST_DELAY  = 0.5                 # seconds between API calls — be respectful
MAX_RETRIES    = 5                   # retry failed requests


# ─── LOAD ACTIVE LOCATIONS

def load_active_locations(source_id):
    # Load all active locations for this source from config
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT
                l.id,
                l.name,
                l.latitude,
                l.longitude,
                l.timezone
            FROM config.source_locations sl
            JOIN config.locations l ON l.id = sl.location_id
            WHERE sl.source_id = %s
              AND sl.is_active  = TRUE
              AND l.is_active   = TRUE
            ORDER BY l.name
        """, (source_id,))
        rows = cur.fetchall()
        return [
            {
                "location_id": row[0],
                "name":        row[1],
                "latitude":    float(row[2]),
                "longitude":   float(row[3]),
                "timezone":    row[4],
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


# ─── CHECK ALREADY BACKFILLED

def get_last_backfilled_date(location_id, source_id):
    """
    Returns the latest observation_at date already in bronze
    for this location. Avoids re-fetching data we already have.
    """
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT MAX(DATE(observation_at))
            FROM bronze.weather
            WHERE location_id = %s
              AND source_id   = %s
        """, (location_id, source_id))
        result = cur.fetchone()[0]
        return result
    finally:
        cur.close()
        conn.close()


# ─── FETCH ONE YEAR FOR ONE LOCATION

def fetch_year(location, source_id, year_start, year_end):
    """
    Fetches one year of hourly data for one location.
    Returns list of row dicts ready for bronze insert.
    """
    params = {
        "latitude":   location["latitude"],
        "longitude":  location["longitude"],
        "start_date": year_start.strftime("%Y-%m-%d"),
        "end_date":   year_end.strftime("%Y-%m-%d"),
        "hourly":     ",".join(HOURLY_PARAMS),
        "timezone":   location["timezone"],
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                HISTORICAL_API_URL,
                params  = params,
                timeout = 60
            )
            response.raise_for_status()
            raw = response.json()

            if "error" in raw:
                logger.warning(
                    f"API error for {location['name']} "
                    f"{year_start.year}: {raw.get('reason')}"
                )
                return []

            # build one row per hour
            rows       = []
            hourly     = raw.get("hourly", {})
            times      = hourly.get("time", [])

            for i, obs_time in enumerate(times):
                # build current block matching live API format
                current_data = {
                    field: hourly.get(field, [None])[i]
                    for field in HOURLY_PARAMS
                }
                current_data["time"] = obs_time

                row = {
                    "source_id":    source_id,
                    "location_id":  location["location_id"],
                    "observation_at": obs_time,
                    "raw_data": json.dumps({
                        "latitude":       raw.get("latitude"),
                        "longitude":      raw.get("longitude"),
                        "timezone":       raw.get("timezone"),
                        "hourly":         {f: [hourly.get(f, [])[i]] for f in HOURLY_PARAMS},
                        "hourly_units":   raw.get("hourly_units", {}),
                        "current":        current_data,
                        "current_units":  raw.get("hourly_units", {}),
                        "data_type":      "historical",
                    })
                }
                rows.append(row)

            logger.info(
                f"  fetched {location['name']} "
                f"{year_start.year} → {len(rows)} hourly rows"
            )
            time.sleep(REQUEST_DELAY)
            return rows

        except requests.exceptions.Timeout:
            logger.warning(
                f"  timeout {location['name']} "
                f"{year_start.year} — attempt {attempt}/{MAX_RETRIES}"
            )
            time.sleep(2 ** attempt)    # exponential backoff

        except Exception as e:
            logger.warning(
                f"  error {location['name']} "
                f"{year_start.year} attempt {attempt}: {e}"
            )
            time.sleep(2 ** attempt)

    logger.error(
        f"  failed {location['name']} "
        f"{year_start.year} after {MAX_RETRIES} attempts"
    )
    return []


# ─── INSERT BATCH INTO BRONZE

def insert_batch(rows, cur):
    """Batch insert rows into bronze.weather"""
    if not rows:
        return 0

    cur.executemany("""
        INSERT INTO bronze.weather (
            source_id,
            location_id,
            observation_at,
            raw_data,
            ingested_at
        ) VALUES (
            %(source_id)s,
            %(location_id)s,
            %(observation_at)s,
            %(raw_data)s,
            NOW()
        )
        ON CONFLICT DO NOTHING
    """, rows)

    return len(rows)


# ─── BACKFILL ONE LOCATION

def backfill_location(location, source_id):
    """
    Backfills all years from START_DATE to today for one location.
    Skips years already in the database.
    """
    name       = location["name"]
    today      = date.today()

    # check what's already loaded
    last_date  = get_last_backfilled_date(
        location["location_id"], source_id
    )

    if last_date:
        start = last_date + timedelta(days=1)
        logger.info(f"  {name} — resuming from {start} (last: {last_date})")
    else:
        start = START_DATE
        logger.info(f"  {name} — starting full backfill from {start}")

    if start >= today:
        logger.info(f"  {name} — already up to date, skipping")
        return 0

    total_inserted = 0
    conn           = get_connection()
    cur            = conn.cursor()

    try:
        current = start
        while current < today:
            # fetch one year at a time
            year_end = min(
                date(current.year, 12, 31),
                today - timedelta(days=1)
            )

            # ensure partition exists for this year
            get_or_create_partition(cur, current.year)
            conn.commit()

            # fetch from API
            rows = fetch_year(location, source_id, current, year_end)

            # insert into bronze
            if rows:
                inserted = insert_batch(rows, cur)
                conn.commit()
                total_inserted += inserted
                logger.info(
                    f"  {name} {current.year} → "
                    f"inserted {inserted} rows"
                )

            # move to next year
            current = date(current.year + 1, 1, 1)

        logger.success(
            f"  {name} — backfill complete: "
            f"{total_inserted} total rows inserted"
        )
        return total_inserted

    except Exception as e:
        conn.rollback()
        logger.error(f"  {name} — backfill failed: {e}")
        return 0

    finally:
        cur.close()
        conn.close()


# ─── MAIN

def run_backfill(source_id=1):
    start_time = time.time()

    logger.info(f"Starting historical backfill")
    logger.info(f"Date range : {START_DATE} → {date.today()}")
    logger.info(f"Parallelism: {MAX_WORKERS} workers")

    # load all 25 provinces
    locations = load_active_locations(source_id)
    logger.info(f"Locations  : {len(locations)} provinces")

    if not locations:
        logger.error("No active locations found — aborting")
        return

    print(f"\n{'═'*60}")
    print(f"  HISTORICAL BACKFILL — {len(locations)} provinces")
    print(f"  From: {START_DATE}  To: {date.today()}")
    print(f"{'═'*60}\n")

    total_rows    = 0
    failed_locs   = []

    # run backfill with limited parallelism
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                backfill_location, loc, source_id
            ): loc["name"]
            for loc in locations
        }

        for future in as_completed(futures):
            name = futures[future]
            try:
                rows_inserted = future.result()
                total_rows   += rows_inserted
            except Exception as e:
                logger.error(f"{name} failed: {e}")
                failed_locs.append(name)

    elapsed = round(time.time() - start_time, 1)

    print(f"\n{'═'*60}")
    print(f"  BACKFILL COMPLETE")
    print(f"{'═'*60}")
    print(f"  total rows inserted : {total_rows:,}")
    print(f"  failed locations    : {len(failed_locs)}")
    print(f"  elapsed time        : {elapsed}s")
    if failed_locs:
        print(f"  failed              : {', '.join(failed_locs)}")
    print(f"{'═'*60}\n")

    # final row count per partition
    print("── Row counts per partition ──────────────────")
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT
                tableoid::regclass  AS partition,
                COUNT(*)            AS rows,
                MIN(observation_at) AS earliest,
                MAX(observation_at) AS latest
            FROM bronze.weather
            GROUP BY tableoid::regclass
            ORDER BY partition
        """)
        rows = cur.fetchall()
        print(f"\n  {'Partition':<28} {'Rows':>10}  {'Earliest':<22} Latest")
        print(f"  {'─'*80}")
        for row in rows:
            print(
                f"  {str(row[0]):<28} "
                f"{row[1]:>10,}  "
                f"{str(row[2]):<22} "
                f"{row[3]}"
            )
        print(f"\n  Total: {sum(r[1] for r in rows):,} rows\n")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run_backfill(source_id=1)