WITH silver AS (

    SELECT *
    FROM {{ ref('stg_weather') }}

)

SELECT

    location_id,
    location_name,
    region,

    observation_date,

    season,
    is_rainy_season,

    COUNT(*)                           AS observations,

    AVG(temperature_c)                 AS avg_temperature_c,
    MIN(temperature_c)                 AS min_temperature_c,
    MAX(temperature_c)                 AS max_temperature_c,

    AVG(humidity_pct)                  AS avg_humidity_pct,

    SUM(precipitation_mm)              AS total_precipitation_mm,

    AVG(wind_speed_kmh)                AS avg_wind_speed_kmh,
    MAX(wind_gusts_kmh)                AS max_wind_gust_kmh,

    AVG(pressure_hpa)                  AS avg_pressure_hpa,

    AVG(cloud_cover_pct)               AS avg_cloud_cover_pct,

    AVG(heat_index_c)                  AS avg_heat_index_c

FROM silver

GROUP BY

    location_id,
    location_name,
    region,

    observation_date,

    season,
    is_rainy_season