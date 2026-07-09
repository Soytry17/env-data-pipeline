from datetime import datetime
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db import get_connection


# ─── BRONZE SCHEMA

CREATE_BRONZE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS bronze;"

GRANT_BRONZE = """
GRANT ALL ON SCHEMA bronze TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
    GRANT ALL ON TABLES TO pipeline_user;
"""

# ─── PARENT TABLE

CREATE_BRONZE_WEATHER = """
CREATE TABLE bronze.weather (
    id               BIGSERIAL,
    source_id        INTEGER NOT NULL,
    location_id      INTEGER NOT NULL,
    observation_at TIMESTAMP NOT NULL,
    raw_data         JSONB NOT NULL,
    ingested_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    data_type        VARCHAR(20) NOT NULL DEFAULT 'live',
    PRIMARY KEY (id, observation_time)
)
PARTITION BY RANGE (observation_time);
"""

# ─── INDEXES

CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_bronze_weather_location_id
    ON bronze.weather (location_id);

CREATE INDEX IF NOT EXISTS idx_bronze_weather_source_id
    ON bronze.weather (source_id);

CREATE INDEX IF NOT EXISTS idx_bronze_weather_extracted_at
    ON bronze.weather (observation_time DESC);

CREATE INDEX IF NOT EXISTS idx_bronze_weather_created_at
    ON bronze.weather (ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_bronze_weather_raw_data
    ON bronze.weather USING GIN (raw_data);
"""

# ─── COMMENTS

ADD_COMMENTS = """
COMMENT ON TABLE  bronze.weather              IS 'Raw weather data — append only, partitioned by year, never modified';
COMMENT ON COLUMN bronze.weather.source_id    IS 'FK to config.sources — which API provided this data';
COMMENT ON COLUMN bronze.weather.location_id  IS 'FK to config.locations — which province';
COMMENT ON COLUMN bronze.weather.observation_time IS 'Timestamp of the reading from the API';
COMMENT ON COLUMN bronze.weather.raw_data     IS 'Complete raw API response as JSONB — nothing interpreted or dropped';
COMMENT ON COLUMN bronze.weather.ingested_at   IS 'When this row was loaded into the database';
"""


def create_yearly_partition(cur, year):
    """Creates one partition for a given year"""
    table_name  = f"bronze.weather_{year}"
    start_date  = f"{year}-01-01"
    end_date    = f"{year + 1}-01-01"

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        PARTITION OF bronze.weather
        FOR VALUES FROM ('{start_date}') TO ('{end_date}');
    """)
    return table_name


def setup_bronze():
    conn = get_connection()
    cur  = conn.cursor()

    try:
        # ── create schema
        print("── Creating bronze schema...")
        cur.execute(CREATE_BRONZE_SCHEMA)
        cur.execute(GRANT_BRONZE)
        conn.commit()
        print("   ✔ Schema created")

        # ── create parent table
        print("\n── Creating bronze.weather (partitioned parent table)...")
        cur.execute(CREATE_BRONZE_WEATHER)
        conn.commit()
        print("   ✔ Parent table created")

        # ── create yearly partitions 2000 → 2026
        print("\n── Creating yearly partitions (2000 → 2026)...")
        partitions_created = []

        current_year = datetime.now().year

        for year in range(2000, current_year+1):
            table_name = create_yearly_partition(cur, year)
            partitions_created.append((year, table_name))
            print(f"   ✔ {table_name}")

        conn.commit()
        print(f"\n   Total: {len(partitions_created)} partitions created")

        # ── create indexes
        print("\n── Creating indexes...")
        cur.execute(CREATE_INDEXES)
        conn.commit()
        print("   ✔ Indexes created")

        # ── add comments
        print("\n── Adding column comments...")
        cur.execute(ADD_COMMENTS)
        conn.commit()
        print("   ✔ Comments added")

        # ── verification
        print("\n── Verification:")
        cur.execute("""
            SELECT
                nmsp_parent.nspname  AS schema,
                parent.relname       AS parent_table,
                child.relname        AS partition,
                pg_get_expr(
                    child.relpartbound,
                    child.oid
                )                    AS partition_range
            FROM pg_inherits
            JOIN pg_class  parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class  child  ON pg_inherits.inhrelid  = child.oid
            JOIN pg_namespace nmsp_parent
                ON nmsp_parent.oid = parent.relnamespace
            WHERE parent.relname = 'weather'
              AND nmsp_parent.nspname = 'bronze'
            ORDER BY child.relname;
        """)
        partitions = cur.fetchall()

        print(f"\n{'─'*65}")
        print(f"  {'Schema':<10} {'Parent':<16} {'Partition':<24} Range")
        print(f"{'─'*65}")
        for row in partitions[:5]:
            print(f"  {row[0]:<10} {row[1]:<16} {row[2]:<24} {row[3]}")
        print(f"  ... ({len(partitions)} total partitions)")
        print(f"{'─'*65}")

        # ── check indexes
        print("\n── Indexes on bronze.weather:")
        cur.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'bronze'
              AND tablename  = 'weather'
            ORDER BY indexname;
        """)
        indexes = cur.fetchall()
        for idx in indexes:
            print(f"   ✔ {idx[0]}")

        print("\n── bronze.weather is ready!\n")

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] {e}")
        raise
    finally:
        cur.close()
        conn.close()


def add_future_partition(year):
    """
    Call this every January to add the next year partition.
    Example: add_future_partition(2027)
    """
    conn = get_connection()
    cur  = conn.cursor()
    try:
        table_name = create_yearly_partition(cur, year)
        conn.commit()
        print(f"✔ Added partition: {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    setup_bronze()