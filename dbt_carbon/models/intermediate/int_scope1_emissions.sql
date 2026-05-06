-- models/intermediate/int_scope1_emissions.sql
-- ─────────────────────────────────────────────────────────────
-- Scope 1: direct emissions จากการเผาไหม้ใน kiln โดยตรง
-- รวม CO2 จาก coal combustion + clinker calcination
-- ─────────────────────────────────────────────────────────────
{{ config(materialized='table') }}

WITH sensor_data AS (
    SELECT * FROM {{ ref('stg_sensor_readings') }}
    WHERE co2_kg_per_t IS NOT NULL
),
daily_scope1 AS (
    SELECT
        reading_date,
        plant_id,

        -- CO2 intensity (kg CO2 per tonne of cement)
        AVG(co2_kg_per_t)                       AS scope1_intensity_avg,
        MIN(co2_kg_per_t)                       AS scope1_intensity_min,
        MAX(co2_kg_per_t)                       AS scope1_intensity_max,
        STDDEV(co2_kg_per_t)                    AS scope1_intensity_std,

        -- Heat consumption
        AVG(heat_mj_per_t)                      AS heat_consumption_avg,

        -- Coal usage
        SUM(coal_t_per_day)                     AS coal_total_t_day,

        -- Data quality
        COUNT(*)                                AS reading_count,
        COUNT(*) FILTER (WHERE is_anomaly)      AS anomaly_count,

        -- Rolling 7-day average (trend)
        AVG(AVG(co2_kg_per_t)) OVER (
            PARTITION BY plant_id
            ORDER BY reading_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                                       AS scope1_rolling_7d,

        -- Rolling 30-day average (baseline)
        AVG(AVG(co2_kg_per_t)) OVER (
            PARTITION BY plant_id
            ORDER BY reading_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )                                       AS scope1_rolling_30d,

        -- Month-over-month change
        AVG(co2_kg_per_t) - LAG(
            AVG(co2_kg_per_t), 30
        ) OVER (
            PARTITION BY plant_id
            ORDER BY reading_date
        )                                       AS scope1_mom_change

    FROM sensor_data
    GROUP BY reading_date, plant_id
)
SELECT
    *,
    -- Flag วันที่ค่าเกิน SCG target 2030
    scope1_intensity_avg > {{ var('net_zero_target_2030') }}
        AS exceeds_2030_target

FROM daily_scope1
ORDER BY reading_date DESC