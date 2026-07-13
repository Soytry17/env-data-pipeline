{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

WITH silver AS (

    SELECT * FROM {{ ref('stg_weather') }}

),

daily_agg AS (

    SELECT
        -- ── identifiers ───────────────────────────────────
        location_id,
        location_name,
        region,
        elevation_m,
        population,
        latitude,
        longitude,
        source_name,

        -- ── time dimensions ───────────────────────────────
        observation_date                                AS date,
        observation_year                                AS year,
        observation_month                               AS month,
        season,
        is_rainy_season,

        -- ── temperature ───────────────────────────────────
        ROUND(AVG(temperature_c)::NUMERIC,  2)          AS avg_temp_c,
        ROUND(MIN(temperature_c)::NUMERIC,  2)          AS min_temp_c,
        ROUND(MAX(temperature_c)::NUMERIC,  2)          AS max_temp_c,
        ROUND((
            MAX(temperature_c) - MIN(temperature_c)
        )::NUMERIC, 2)                                  AS temp_range_c,

        -- ── humidity ──────────────────────────────────────
        ROUND(AVG(humidity_pct)::NUMERIC,   2)          AS avg_humidity_pct,
        ROUND(MIN(humidity_pct)::NUMERIC,   2)          AS min_humidity_pct,
        ROUND(MAX(humidity_pct)::NUMERIC,   2)          AS max_humidity_pct,

        -- ── wind ──────────────────────────────────────────
        ROUND(AVG(wind_speed_kmh)::NUMERIC, 2)          AS avg_wind_speed_kmh,
        ROUND(MAX(wind_speed_kmh)::NUMERIC, 2)          AS max_wind_speed_kmh,
        ROUND(AVG(wind_gusts_kmh)::NUMERIC, 2)          AS avg_wind_gusts_kmh,
        ROUND(AVG(wind_direction_deg)::NUMERIC, 1)      AS avg_wind_direction_deg,

        -- ── precipitation ─────────────────────────────────
        ROUND(SUM(precipitation_mm)::NUMERIC, 2)        AS total_precipitation_mm,
        ROUND(MAX(precipitation_mm)::NUMERIC, 2)        AS max_hourly_precipitation_mm,
        SUM(
            CASE WHEN precipitation_mm > 0
            THEN 1 ELSE 0 END
        )                                               AS rainy_hours_count,
        CASE
            WHEN SUM(precipitation_mm) > 0
            THEN TRUE ELSE FALSE
        END                                             AS is_rainy_day,

        -- ── pressure and visibility ────────────────────────
        ROUND(AVG(pressure_hpa)::NUMERIC,    2)         AS avg_pressure_hpa,
        ROUND(AVG(cloud_cover_pct)::NUMERIC, 2)         AS avg_cloud_cover_pct,
        ROUND(AVG(visibility_m)::NUMERIC,    0)         AS avg_visibility_m,

        -- ── heat index ────────────────────────────────────
        ROUND(AVG(heat_index_c)::NUMERIC,    2)         AS avg_heat_index_c,
        ROUND(MAX(heat_index_c)::NUMERIC,    2)         AS max_heat_index_c,

        -- ── dominant weather description ───────────────────
        MODE() WITHIN GROUP (
            ORDER BY weather_description
        )                                               AS dominant_weather_description,

        MODE() WITHIN GROUP (
            ORDER BY comfort_level
        )                                               AS dominant_comfort_level,

        -- ── reading quality ───────────────────────────────
        COUNT(*)                                        AS hourly_readings_count,
        COUNT(DISTINCT observation_hour)                AS hours_covered

    FROM silver
    WHERE observation_date IS NOT NULL
      AND temperature_c    IS NOT NULL
    GROUP BY
        location_id,
        location_name,
        region,
        elevation_m,
        population,
        latitude,
        longitude,
        source_name,
        observation_date,
        observation_year,
        observation_month,
        season,
        is_rainy_season

),

with_classifications AS (

    SELECT
        *,

        -- ── day classification ────────────────────────────
        CASE
            WHEN avg_temp_c >= 35
             AND avg_humidity_pct >= 80   THEN 'Extreme Heat'
            WHEN avg_temp_c >= 32
             AND avg_humidity_pct >= 70   THEN 'Hot and Humid'
            WHEN avg_temp_c >= 28         THEN 'Warm'
            WHEN avg_temp_c >= 20         THEN 'Mild'
            ELSE                               'Cool'
        END                                             AS day_classification,

        -- ── precipitation classification ──────────────────
        CASE
            WHEN total_precipitation_mm = 0    THEN 'No rain'
            WHEN total_precipitation_mm < 5    THEN 'Light rain'
            WHEN total_precipitation_mm < 20   THEN 'Moderate rain'
            WHEN total_precipitation_mm < 50   THEN 'Heavy rain'
            ELSE                                    'Extreme rain'
        END                                             AS precipitation_category,

        -- ── wind classification ───────────────────────────
        CASE
            WHEN avg_wind_speed_kmh < 12   THEN 'Calm'
            WHEN avg_wind_speed_kmh < 29   THEN 'Breeze'
            WHEN avg_wind_speed_kmh < 50   THEN 'Moderate wind'
            WHEN avg_wind_speed_kmh < 75   THEN 'Strong wind'
            ELSE                                'Storm'
        END                                             AS wind_classification,

        -- ── metadata ──────────────────────────────────────
        NOW()                                           AS dbt_updated_at

    FROM daily_agg

)

SELECT * FROM with_classifications
ORDER BY date DESC, location_name