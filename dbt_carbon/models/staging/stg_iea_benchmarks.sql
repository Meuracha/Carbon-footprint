-- models/staging/stg_iea_benchmarks.sql
-- ─────────────────────────────────────────────────────────────
-- Clean IEA cement sector benchmarks
-- ─────────────────────────────────────────────────────────────
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('carbon_raw', 'raw_iea_benchmarks') }}
),
pivoted AS (
    SELECT
        year,
        sector,
        region,
        -- Pivot metrics เป็น columns
        MAX(CASE WHEN metric = 'co2_intensity'
            THEN value END)                     AS co2_intensity_kg_per_t,
        MAX(CASE WHEN metric = 'heat_consumption'
            THEN value END)                     AS heat_consumption_mj_per_t,
        ingested_at
    FROM source
    WHERE sector = 'cement'
    GROUP BY year, sector, region, ingested_at
)
SELECT
    year,
    sector,
    region,
    co2_intensity_kg_per_t,
    heat_consumption_mj_per_t,

    -- Net Zero targets จาก vars
    {{ var('net_zero_target_2030') }}           AS scg_target_2030,
    {{ var('net_zero_target_2050') }}           AS scg_target_2050,
    {{ var('net_zero_baseline_intensity') }}    AS baseline_2020,

    ingested_at
FROM pivoted
ORDER BY region, year