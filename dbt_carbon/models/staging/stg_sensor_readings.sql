-- models/staging/stg_sensor_readings.sql
-- ─────────────────────────────────────────────────────────────
-- Clean และ standardize sensor readings จาก kiln pipeline
-- แปลง unit ให้เป็น standard สำหรับ carbon calculation
-- ─────────────────────────────────────────────────────────────
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('carbon_raw', 'raw_sensor_readings') }}
),
cleaned AS (
    SELECT
        reading_date,
        plant_id,
        sensor,
        value                                   AS raw_value,
        unit_label                              AS raw_unit,
        is_anomaly,
        ingested_at,

        -- Standardize carbon-related sensors → kg CO2/t-cement
        CASE
            WHEN sensor = 'CO2_emission_intensity'
                THEN value
            WHEN sensor = 'specific_heat_consumption'
                -- kcal/kg-clinker → kg CO2/t-cement
                -- coal emission factor: 0.0946 kg CO2/kcal
                THEN value * 0.0946 * 0.95     -- clinker-to-cement ratio 0.95
            ELSE NULL
        END                                     AS co2_kg_per_t,

        -- Energy ใน MJ/t-clinker (สำหรับ IEA comparison)
        CASE
            WHEN sensor = 'specific_heat_consumption'
                THEN value * 4.184 / 1000      -- kcal → MJ
            ELSE NULL
        END                                     AS heat_mj_per_t,

        -- Coal consumption ใน t/day
        CASE
            WHEN sensor = 'coal_consumption_rate'
                THEN value * 24                -- t/hr → t/day
            ELSE NULL
        END                                     AS coal_t_per_day

    FROM source
    WHERE
        value IS NOT NULL
        AND value > 0
        AND sensor IN (
            'CO2_emission_intensity',
            'specific_heat_consumption',
            'coal_consumption_rate',
            'NOx_emission',
            'dust_emission',
            'SO2_emission',
            'O2_flue_gas'
        )
)
SELECT * FROM cleaned