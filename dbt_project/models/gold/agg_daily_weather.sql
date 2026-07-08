WITH silver AS (

    SELECT * FROM {{ ref('stg_weather') }}

),

daily_stats AS (

    SELECT
        location,
        reading_date                            AS date,

        -- temperature stats
        ROUND(AVG(temperature_c)::NUMERIC, 2)   AS avg_temp_c,
        ROUND(MIN(temperature_c)::NUMERIC, 2)   AS min_temp_c,
        ROUND(MAX(temperature_c)::NUMERIC, 2)   AS max_temp_c,

        -- humidity stats
        ROUND(AVG(humidity_pct)::NUMERIC, 2)    AS avg_humidity_pct,
        ROUND(MIN(humidity_pct)::NUMERIC, 2)    AS min_humidity_pct,
        ROUND(MAX(humidity_pct)::NUMERIC, 2)    AS max_humidity_pct,

        -- wind stats
        ROUND(AVG(wind_speed_kmh)::NUMERIC, 2)  AS avg_wind_speed_kmh,
        ROUND(MAX(wind_speed_kmh)::NUMERIC, 2)  AS max_wind_speed_kmh,

        -- precipitation
        ROUND(SUM(precipitation_mm)::NUMERIC, 2) AS total_precipitation_mm,

        -- heat index
        ROUND(AVG(heat_index_c)::NUMERIC, 2)    AS avg_heat_index_c,
        ROUND(MAX(heat_index_c)::NUMERIC, 2)    AS max_heat_index_c,

        -- most common comfort level of the day
        MODE() WITHIN GROUP (
            ORDER BY comfort_level
        )                                       AS dominant_comfort_level,

        -- reading counts
        COUNT(*)                                AS total_readings,
        COUNT(DISTINCT reading_hour)            AS hours_covered

    FROM silver
    WHERE temperature_c IS NOT NULL
    GROUP BY location, reading_date

)

SELECT
    *,
    -- classify the day overall
    CASE
        WHEN avg_temp_c >= 35 AND avg_humidity_pct >= 80 THEN 'Extreme Heat'
        WHEN avg_temp_c >= 30 AND avg_humidity_pct >= 70 THEN 'Hot and Humid'
        WHEN avg_temp_c >= 25                            THEN 'Warm'
        WHEN avg_temp_c >= 15                            THEN 'Mild'
        ELSE 'Cool'
    END                                         AS day_classification,

    NOW()                                       AS dbt_updated_at

FROM daily_stats
ORDER BY date DESC, location