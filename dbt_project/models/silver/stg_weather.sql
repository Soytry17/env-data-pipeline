-- Silver layer: unpack JSONB → clean typed columns
-- joins config tables to enrich with location metadata

WITH bronze AS (

    SELECT * FROM {{ source('bronze', 'weather') }}

),

config_locations AS (

    SELECT * FROM {{ source('config', 'locations') }}

),

config_sources AS (

    SELECT * FROM {{ source('config', 'sources') }}

),

unpacked AS (

    SELECT
        b.id                                                AS bronze_id,
        b.source_id,
        b.location_id,
        b.ingested_at,

        -- ── timestamps
        b.observation_at::TIMESTAMP                         AS observation_at,
        DATE(b.observation_at)                              AS observation_date,
        EXTRACT(HOUR FROM b.observation_at)::INT            AS observation_hour,
        EXTRACT(MONTH FROM b.observation_at)::INT           AS observation_month,
        EXTRACT(YEAR FROM b.observation_at)::INT            AS observation_year,

        -- ── location metadata from config
        l.name                                              AS location_name,
        l.name_khmer,
        l.region,
        l.latitude,
        l.longitude,
        l.elevation_m,
        l.population,
        l.timezone,

        -- ── source metadata
        s.source_name,

        -- ── unpack raw_data JSONB current block
        -- temperature
        (b.raw_data->'current'->>'temperature_2m')
            ::NUMERIC(5,2)                                  AS temperature_c,

        -- humidity
        (b.raw_data->'current'->>'relative_humidity_2m')
            ::NUMERIC(5,2)                                  AS humidity_pct,

        -- wind
        (b.raw_data->'current'->>'wind_speed_10m')
            ::NUMERIC(6,2)                                  AS wind_speed_kmh,

        (b.raw_data->'current'->>'wind_direction_10m')
            ::NUMERIC(5,1)                                  AS wind_direction_deg,

        (b.raw_data->'current'->>'wind_gusts_10m')
            ::NUMERIC(6,2)                                  AS wind_gusts_kmh,

        -- precipitation
        (b.raw_data->'current'->>'precipitation')
            ::NUMERIC(6,2)                                  AS precipitation_mm,

        -- atmospheric
        (b.raw_data->'current'->>'surface_pressure')
            ::NUMERIC(7,2)                                  AS pressure_hpa,

        (b.raw_data->'current'->>'cloud_cover')
            ::NUMERIC(5,2)                                  AS cloud_cover_pct,

        (b.raw_data->'current'->>'visibility')
            ::NUMERIC(10,2)                                 AS visibility_m,

        -- weather code
        (b.raw_data->'current'->>'weather_code')
            ::INTEGER                                       AS weather_code,

        -- ── unpack units from raw_data
        b.raw_data->'current_units'->>'temperature_2m'      AS unit_temperature,
        b.raw_data->'current_units'->>'wind_speed_10m'      AS unit_wind_speed,
        b.raw_data->'current_units'->>'precipitation'       AS unit_precipitation

    FROM bronze b
    JOIN config_locations l ON l.id = b.location_id
    JOIN config_sources   s ON s.id = b.source_id

),

with_descriptions AS (

    SELECT
        *,
        -- ── weather description from WMO code
        CASE weather_code
            WHEN 0              THEN 'Clear sky'
            WHEN 1              THEN 'Mainly clear'
            WHEN 2              THEN 'Partly cloudy'
            WHEN 3              THEN 'Overcast'
            WHEN 45             THEN 'Foggy'
            WHEN 48             THEN 'Icy fog'
            WHEN 51             THEN 'Light drizzle'
            WHEN 53             THEN 'Moderate drizzle'
            WHEN 55             THEN 'Heavy drizzle'
            WHEN 61             THEN 'Slight rain'
            WHEN 63             THEN 'Moderate rain'
            WHEN 65             THEN 'Heavy rain'
            WHEN 71             THEN 'Slight snow'
            WHEN 73             THEN 'Moderate snow'
            WHEN 75             THEN 'Heavy snow'
            WHEN 80             THEN 'Slight showers'
            WHEN 81             THEN 'Moderate showers'
            WHEN 82             THEN 'Heavy showers'
            WHEN 95             THEN 'Thunderstorm'
            WHEN 96             THEN 'Thunderstorm with hail'
            WHEN 99             THEN 'Thunderstorm with heavy hail'
            ELSE                     'Unknown'
        END                                                 AS weather_description,

        -- ── cambodia rainy season (May → October)
        CASE
            WHEN observation_month BETWEEN 5 AND 10
            THEN 'rainy'
            ELSE 'dry'
        END                                                 AS season,

        CASE
            WHEN observation_month BETWEEN 5 AND 10
            THEN TRUE
            ELSE FALSE
        END                                                 AS is_rainy_season,

        -- ── heat index (simplified Steadman formula)
        ROUND((
            -8.78469475556
            + 1.61139411    * temperature_c
            + 2.3385491     * humidity_pct
            - 0.14611605    * temperature_c * humidity_pct
            - 0.012308094   * temperature_c * temperature_c
            - 0.016424828   * humidity_pct  * humidity_pct
            + 0.002211732   * temperature_c * temperature_c * humidity_pct
            + 0.00072546    * temperature_c * humidity_pct  * humidity_pct
            - 0.000003582   * temperature_c * temperature_c
                            * humidity_pct  * humidity_pct
        )::NUMERIC, 2)                                      AS heat_index_c,

        -- ── comfort level ────────────────────────────────
        CASE
            WHEN temperature_c IS NULL OR humidity_pct IS NULL
                THEN 'Unknown'
            WHEN temperature_c >= 35 AND humidity_pct >= 80
                THEN 'Extreme Danger'
            WHEN temperature_c >= 32 AND humidity_pct >= 70
                THEN 'Danger'
            WHEN temperature_c >= 29 AND humidity_pct >= 60
                THEN 'Extreme Caution'
            WHEN temperature_c >= 27
                THEN 'Caution'
            ELSE 'Comfortable'
        END                                                 AS comfort_level

    FROM unpacked

),

deduplicated AS (

    -- keep only the latest ingested row
    -- per location per observation hour
    SELECT DISTINCT ON (location_id, observation_date, observation_hour)
        *
    FROM with_descriptions
    WHERE observation_at IS NOT NULL
      AND location_id    IS NOT NULL
      AND source_id      IS NOT NULL
    ORDER BY
        location_id,
        observation_date,
        observation_hour,
        ingested_at DESC

)

SELECT * FROM deduplicated