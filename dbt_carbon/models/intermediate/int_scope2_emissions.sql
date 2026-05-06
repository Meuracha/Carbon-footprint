-- models/intermediate/int_scope2_emissions.sql
-- ─────────────────────────────────────────────────────────────
-- Scope 2: indirect emissions จาก purchased electricity
-- ใช้ Thailand grid emission factor จาก EGAT
-- ─────────────────────────────────────────────────────────────
{{ config(materialized='table') }}

WITH power_data AS (
    SELECT
        reading_date,
        plant_id,
        -- mill_power_draw เป็น proxy สำหรับ electricity consumption
        -- ในโปรเจคจริง: ดึงจาก electricity meter โดยตรง
        AVG(raw_value) FILTER (WHERE sensor = 'O2_flue_gas')
            AS o2_avg,
        COUNT(*) AS reading_count
    FROM {{ ref('stg_sensor_readings') }}
    GROUP BY reading_date, plant_id
),
scope2 AS (
    SELECT
        reading_date,
        plant_id,

        -- Thailand grid emission factor (EGAT 2023): 0.4999 kg CO2/kWh
        -- Estimated electricity usage: ~110 kWh/t-cement
        110.0 * 0.4999                          AS scope2_intensity_kg_per_t,

        -- Electricity emission factor (Thailand)
        0.4999                                  AS grid_emission_factor_kg_per_kwh,

        reading_count
    FROM power_data
)
SELECT * FROM scope2
ORDER BY reading_date DESC