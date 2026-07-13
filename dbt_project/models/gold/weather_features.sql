{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

WITH silver AS (

    SELECT * FROM {{ ref('stg_weather') }}

),

-- ── STEP 1: daily aggregations per province
daily AS (

    SELECT
        location_id,
        location_name,
        region,
        elevation_m,
        population,
        latitude,
        longitude,
        observation_date                            AS feature_date,
        observation_year                            AS year,
        observation_month                           AS month,
        season,
        is_rainy_season,

        -- temperature features
        ROUND(AVG(temperature_c)::NUMERIC,  2)      AS avg_temp_c,
        ROUND(MIN(temperature_c)::NUMERIC,  2)      AS min_temp_c,
        ROUND(MAX(temperature_c)::NUMERIC,  2)      AS max_temp_c,
        ROUND((
            MAX(temperature_c) - MIN(temperature_c)
        )::NUMERIC, 2)                              AS temp_range_c,

        -- humidity features
        ROUND(AVG(humidity_pct)::NUMERIC,   2)      AS avg_humidity_pct,
        ROUND(MIN(humidity_pct)::NUMERIC,   2)      AS min_humidity_pct,
        ROUND(MAX(humidity_pct)::NUMERIC,   2)      AS max_humidity_pct,

        -- wind features
        ROUND(AVG(wind_speed_kmh)::NUMERIC, 2)      AS avg_wind_speed_kmh,
        ROUND(MAX(wind_speed_kmh)::NUMERIC, 2)      AS max_wind_speed_kmh,
        ROUND(AVG(wind_gusts_kmh)::NUMERIC, 2)      AS avg_wind_gusts_kmh,

        -- precipitation features
        ROUND(SUM(precipitation_mm)::NUMERIC, 2)    AS total_precipitation_mm,
        ROUND(MAX(precipitation_mm)::NUMERIC, 2)    AS max_hourly_precipitation_mm,
        SUM(
            CASE WHEN precipitation_mm > 0
            THEN 1 ELSE 0 END
        )                                           AS rainy_hours_count,

        -- pressure and cloud
        ROUND(AVG(pressure_hpa)::NUMERIC,   2)      AS avg_pressure_hpa,
        ROUND(AVG(cloud_cover_pct)::NUMERIC, 2)     AS avg_cloud_cover_pct,

        -- heat index
        ROUND(AVG(heat_index_c)::NUMERIC,   2)      AS avg_heat_index_c,
        ROUND(MAX(heat_index_c)::NUMERIC,   2)      AS max_heat_index_c,

        -- reading quality
        COUNT(*)                                    AS hourly_readings_count,
        COUNT(DISTINCT observation_hour)            AS hours_covered

    FROM silver
    WHERE temperature_c  IS NOT NULL
      AND humidity_pct   IS NOT NULL
      AND observation_date IS NOT NULL
    GROUP BY
        location_id,
        location_name,
        region,
        elevation_m,
        population,
        latitude,
        longitude,
        observation_date,
        observation_year,
        observation_month,
        season,
        is_rainy_season

),

-- ── STEP 2: lag features (previous days) ─────────────────
with_lags AS (

    SELECT
        *,

        -- temperature lags
        LAG(avg_temp_c, 1)  OVER w                 AS temp_lag_1d,
        LAG(avg_temp_c, 2)  OVER w                 AS temp_lag_2d,
        LAG(avg_temp_c, 3)  OVER w                 AS temp_lag_3d,
        LAG(avg_temp_c, 7)  OVER w                 AS temp_lag_7d,
        LAG(avg_temp_c, 14) OVER w                 AS temp_lag_14d,
        LAG(avg_temp_c, 30) OVER w                 AS temp_lag_30d,

        -- humidity lags
        LAG(avg_humidity_pct, 1) OVER w             AS humidity_lag_1d,
        LAG(avg_humidity_pct, 7) OVER w             AS humidity_lag_7d,
        LAG(avg_humidity_pct, 30) OVER w            AS humidity_lag_30d,

        -- precipitation lags
        LAG(total_precipitation_mm, 1) OVER w       AS precip_lag_1d,
        LAG(total_precipitation_mm, 7) OVER w       AS precip_lag_7d,

        -- was it raining yesterday?
        LAG(
            CASE WHEN total_precipitation_mm > 0
            THEN 1 ELSE 0 END, 1
        ) OVER w                                    AS was_rainy_yesterday,

        -- temp change from yesterday
        avg_temp_c - LAG(avg_temp_c, 1) OVER w      AS temp_change_1d,
        avg_temp_c - LAG(avg_temp_c, 7) OVER w      AS temp_change_7d

    FROM daily
    WINDOW w AS (
        PARTITION BY location_id
        ORDER BY feature_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )

),

-- ── STEP 3: rolling averages ──────────────────────────────
with_rolling AS (

    SELECT
        *,

        -- 7-day rolling averages
        ROUND(AVG(avg_temp_c) OVER (
            PARTITION BY location_id
            ORDER BY feature_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2)                              AS rolling_7d_avg_temp,

        ROUND(AVG(avg_humidity_pct) OVER (
            PARTITION BY location_id
            ORDER BY feature_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2)                              AS rolling_7d_avg_humidity,

        ROUND(SUM(total_precipitation_mm) OVER (
            PARTITION BY location_id
            ORDER BY feature_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2)                              AS rolling_7d_total_precip,

        -- 30-day rolling averages
        ROUND(AVG(avg_temp_c) OVER (
            PARTITION BY location_id
            ORDER BY feature_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2)                              AS rolling_30d_avg_temp,

        ROUND(AVG(avg_humidity_pct) OVER (
            PARTITION BY location_id
            ORDER BY feature_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2)                              AS rolling_30d_avg_humidity,

        ROUND(SUM(total_precipitation_mm) OVER (
            PARTITION BY location_id
            ORDER BY feature_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2)                              AS rolling_30d_total_precip,

        -- 90-day rolling averages (seasonal context)
        ROUND(AVG(avg_temp_c) OVER (
            PARTITION BY location_id
            ORDER BY feature_date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2)                              AS rolling_90d_avg_temp,

        -- consecutive rainy days streak
        SUM(
            CASE WHEN total_precipitation_mm > 0
            THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY location_id
            ORDER BY feature_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                                           AS rainy_days_last_7d,

        SUM(
            CASE WHEN total_precipitation_mm > 0
            THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY location_id
            ORDER BY feature_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )                                           AS rainy_days_last_30d

    FROM with_lags

),

-- ── STEP 4: calendar + regional features ─────────────────
with_calendar AS (

    SELECT
        *,

        -- calendar features for ML
        EXTRACT(DOY  FROM feature_date)::INT        AS day_of_year,
        EXTRACT(DOW  FROM feature_date)::INT        AS day_of_week,
        EXTRACT(WEEK FROM feature_date)::INT        AS week_of_year,
        EXTRACT(QUARTER FROM feature_date)::INT     AS quarter,

        -- cyclical encoding of month
        -- converts month to sin/cos so ML model understands
        -- that month 12 and month 1 are close together
        ROUND(
            SIN(2 * PI() * month / 12)::NUMERIC, 6
        )                                           AS month_sin,
        ROUND(
            COS(2 * PI() * month / 12)::NUMERIC, 6
        )                                           AS month_cos,

        -- cyclical encoding of day of year
        ROUND(
            SIN(2 * PI() *
                EXTRACT(DOY FROM feature_date) / 365
            )::NUMERIC, 6
        )                                           AS doy_sin,
        ROUND(
            COS(2 * PI() *
                EXTRACT(DOY FROM feature_date) / 365
            )::NUMERIC, 6
        )                                           AS doy_cos,

        -- region encoding for ML
        CASE region
            WHEN 'Central'  THEN 1
            WHEN 'Northern' THEN 2
            WHEN 'Highland' THEN 3
            WHEN 'Coastal'  THEN 4
            WHEN 'Eastern'  THEN 5
            ELSE 0
        END                                         AS region_code,

        -- elevation category
        CASE
            WHEN elevation_m < 20   THEN 'lowland'
            WHEN elevation_m < 100  THEN 'low_hill'
            WHEN elevation_m < 500  THEN 'hill'
            ELSE                         'highland'
        END                                         AS elevation_category,

        -- temperature anomaly vs 30d rolling
        ROUND((
            avg_temp_c - rolling_30d_avg_temp
        )::NUMERIC, 2)                              AS temp_anomaly_30d,

        -- is this an extreme weather day?
        CASE
            WHEN total_precipitation_mm > 50  THEN TRUE
            ELSE FALSE
        END                                         AS is_heavy_rain_day,

        CASE
            WHEN avg_temp_c > 38              THEN TRUE
            ELSE FALSE
        END                                         AS is_extreme_heat_day,

        -- target variable for AI model
        -- tomorrow's temperature (shift by -1 day)
        LEAD(avg_temp_c, 1) OVER (
            PARTITION BY location_id
            ORDER BY feature_date
        )                                           AS target_next_day_temp,

        LEAD(total_precipitation_mm, 1) OVER (
            PARTITION BY location_id
            ORDER BY feature_date
        )                                           AS target_next_day_precip,

        LEAD(
            CASE WHEN total_precipitation_mm > 0
            THEN 1 ELSE 0 END, 1
        ) OVER (
            PARTITION BY location_id
            ORDER BY feature_date
        )                                           AS target_will_rain_tomorrow,

        -- metadata
        NOW()                                       AS dbt_updated_at

    FROM with_rolling

)

SELECT * FROM with_calendar
ORDER BY feature_date DESC, location_name