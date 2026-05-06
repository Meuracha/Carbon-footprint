-- models/staging/stg_owid_emissions.sql
-- ─────────────────────────────────────────────────────────────
-- Clean Our World in Data CO2 emissions dataset
-- ─────────────────────────────────────────────────────────────
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('carbon_raw', 'raw_owid_emissions') }}
),
cleaned AS (
    SELECT
        year,
        country,
        COALESCE(co2_total_mt, 0)               AS co2_total_mt,
        COALESCE(co2_per_capita, 0)             AS co2_per_capita,
        COALESCE(cement_co2, 0)                 AS cement_co2_mt,
        COALESCE(energy_co2, 0)                 AS energy_co2_mt,

        -- Cement CO2 share of total
        CASE
            WHEN co2_total_mt > 0
                THEN ROUND(
                    (cement_co2 / NULLIF(co2_total_mt, 0) * 100)::numeric, 2
                )
            ELSE NULL
        END                                     AS cement_co2_share_pct,

        ingested_at
    FROM source
    WHERE
        year IS NOT NULL
        AND country IS NOT NULL
        AND co2_total_mt > 0
)
SELECT * FROM cleaned
ORDER BY country, year