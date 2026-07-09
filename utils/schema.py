# Create Schema for 3 layer
CREATE_SCHEMAS = """
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
"""

# BRONZE LAYER
CREATE_BRONZE_WEATHER = """
CREATE TABLE IF NOT EXISTS bronze.weather (
    id                  SERIAL PRIMARY KEY,
    location            VARCHAR(100)    NOT NULL,
    latitude            NUMERIC(9, 6),
    longitude           NUMERIC(9, 6),
    extracted_at        TIMESTAMP,
    processed_at        TIMESTAMP,
    temperature         NUMERIC(5, 2),
    humidity            NUMERIC(5, 2),
    wind_speed          NUMERIC(6, 2),
    precipitation       NUMERIC(6, 2),
    weather_code        INTEGER,
    weather_description VARCHAR(100),
    heat_index          NUMERIC(5, 2),
    comfort_level       VARCHAR(50),
    pipeline_version    VARCHAR(20),
    created_at          TIMESTAMP DEFAULT NOW()
);
"""

CREATE_BRONZE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_bronze_weather_location
    ON bronze.weather (location);

CREATE INDEX IF NOT EXISTS idx_bronze_weather_extracted_at
    ON bronze.weather (extracted_at);

CREATE INDEX IF NOT EXISTS idx_bronze_weather_created_at
    ON bronze.weather (created_at);
"""

# COMMENTS
BRONZE_COMMENTS = """
COMMENT ON SCHEMA bronze IS 'Raw ingested data — append only, never modified';
COMMENT ON SCHEMA silver IS 'Cleaned and deduplicated data — built by dbt';
COMMENT ON SCHEMA gold   IS 'Aggregated analytics layer — built by dbt';
COMMENT ON TABLE  bronze.weather IS 'Raw weather readings from Open-Meteo API';
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