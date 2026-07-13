CREATE_SCHEMAS = """
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
"""

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
    PRIMARY KEY (id, observation_at)
)
PARTITION BY RANGE (observation_at);
"""

# ─── INDEXES

CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_bronze_weather_location_id
    ON bronze.weather (location_id);

CREATE INDEX IF NOT EXISTS idx_bronze_weather_source_id
    ON bronze.weather (source_id);

CREATE INDEX IF NOT EXISTS idx_bronze_weather_observation_at
    ON bronze.weather (observation_at DESC);

CREATE INDEX IF NOT EXISTS idx_bronze_weather_ingested_at
    ON bronze.weather (ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_bronze_weather_raw_data
    ON bronze.weather USING GIN (raw_data);
"""

# ─── COMMENTS

ADD_COMMENTS = """
COMMENT ON TABLE  bronze.weather              IS 'Raw weather data — append only, partitioned by year, never modified';
COMMENT ON COLUMN bronze.weather.source_id    IS 'FK to config.sources — which API provided this data';
COMMENT ON COLUMN bronze.weather.location_id  IS 'FK to config.locations — which province';
COMMENT ON COLUMN bronze.weather.observation_at IS 'Timestamp of the reading from the API';
COMMENT ON COLUMN bronze.weather.raw_data     IS 'Complete raw API response as JSONB — nothing interpreted or dropped';
COMMENT ON COLUMN bronze.weather.ingested_at   IS 'When this row was loaded into the database';
"""



def get_or_create_partition(cur, year):
    """
    Ensures a partition exists for the given year.
    Safe to call multiple times — uses IF NOT EXISTS.
    """
    table_name = f"bronze.weather_{year}"
    start_date = f"{year}-01-01"
    end_date   = f"{year + 1}-01-01"

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        PARTITION OF bronze.weather
        FOR VALUES FROM ('{start_date}') TO ('{end_date}');
    """)
    return table_name