-- models/intermediate/int_scope3_emissions.sql
-- ─────────────────────────────────────────────────────────────
-- Scope 3: value chain emissions (upstream + downstream)
-- - upstream: raw materials (limestone, coal transport)
-- - downstream: cement distribution
-- ─────────────────────────────────────────────────────────────
{{ config(materialized='table') }}

WITH scope1 AS (
    SELECT * FROM {{ ref('int_scope1_emissions') }}
),
scope3 AS (
    SELECT
        reading_date,
        plant_id,

        -- Upstream: raw material extraction + transport
        -- Limestone mining: ~15 kg CO2/t-cement (industry estimate)
        -- Coal transport: ~5 kg CO2/t-cement
        15.0 + 5.0                              AS scope3_upstream_kg_per_t,

        -- Downstream: cement transport to customers
        -- Average distance 200km, truck emission 0.1 kg CO2/t-km
        200.0 * 0.1                             AS scope3_downstream_kg_per_t,

        -- Total Scope 3
        (15.0 + 5.0) + (200.0 * 0.1)           AS scope3_total_kg_per_t,

        -- Note: ค่าเหล่านี้เป็น industry estimates
        -- ในโปรเจคจริงควรดึงจาก LCA (Life Cycle Assessment) data
        'industry_estimate'                      AS data_source

    FROM scope1
)
SELECT * FROM scope3
ORDER BY reading_date DESC