WITH silver AS (

    SELECT *
    FROM {{ ref('stg_weather') }}

)

SELECT

    location_id,
    location_name,
    region,

    observation_at,
    observation_date,

    observation_month,
    observation_year,

    season,
    is_rainy_season,

    temperature_c,
    humidity_pct,
    precipitation_mm,
    wind_speed_kmh,
    pressure_hpa,
    cloud_cover_pct,
    heat_index_c,

    LAG(temperature_c,1)
        OVER(
            PARTITION BY location_id
            ORDER BY observation_at
        ) AS temperature_lag_1,

    LAG(temperature_c,24)
        OVER(
            PARTITION BY location_id
            ORDER BY observation_at
        ) AS temperature_lag_24,

    LAG(precipitation_mm,24)
        OVER(
            PARTITION BY location_id
            ORDER BY observation_at
        ) AS rainfall_lag_24,

    AVG(temperature_c)
        OVER(
            PARTITION BY location_id
            ORDER BY observation_at
            ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
        ) AS rolling_temp_24h,

    AVG(humidity_pct)
        OVER(
            PARTITION BY location_id
            ORDER BY observation_at
            ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
        ) AS rolling_humidity_24h,

    LEAD(temperature_c,24)
        OVER(
            PARTITION BY location_id
            ORDER BY observation_at
        ) AS target_temperature_next_day,

    LEAD(precipitation_mm,24)
        OVER(
            PARTITION BY location_id
            ORDER BY observation_at
        ) AS target_rainfall_next_day

FROM silver