WITH source AS (

    SELECT * FROM {{ source('bronze', 'weather') }}

),

cleaned AS (

    SELECT
        id,
        TRIM(location)                          AS location,
        ROUND(latitude::NUMERIC, 6)             AS latitude,
        ROUND(longitude::NUMERIC, 6)            AS longitude,

        -- clean timestamps
        extracted_at::TIMESTAMP                 AS extracted_at,
        processed_at::TIMESTAMP                 AS processed_at,
        created_at::TIMESTAMP                   AS ingested_at,

        -- measurements with safety bounds
        CASE
            WHEN temperature BETWEEN -20 AND 60
            THEN ROUND(temperature::NUMERIC, 2)
            ELSE NULL
        END                                     AS temperature_c,

        CASE
            WHEN humidity BETWEEN 0 AND 100
            THEN ROUND(humidity::NUMERIC, 2)
            ELSE NULL
        END                                     AS humidity_pct,

        CASE
            WHEN wind_speed BETWEEN 0 AND 200
            THEN ROUND(wind_speed::NUMERIC, 2)
            ELSE NULL
        END                                     AS wind_speed_kmh,

        CASE
            WHEN precipitation BETWEEN 0 AND 500
            THEN ROUND(precipitation::NUMERIC, 2)
            ELSE NULL
        END                                     AS precipitation_mm,

        -- enriched fields
        weather_code,
        weather_description,
        ROUND(heat_index::NUMERIC, 2)           AS heat_index_c,
        comfort_level,
        pipeline_version,

        -- add date parts for easy filtering
        DATE(extracted_at)                      AS reading_date,
        EXTRACT(HOUR FROM extracted_at)::INT    AS reading_hour

    FROM source

),

deduplicated AS (

    -- keep only the latest reading per location per hour
    SELECT DISTINCT ON (location, reading_date, reading_hour)
        *
    FROM cleaned
    WHERE location IS NOT NULL
      AND extracted_at IS NOT NULL
    ORDER BY
        location,
        reading_date,
        reading_hour,
        ingested_at DESC          -- latest wins

)

SELECT * FROM deduplicated