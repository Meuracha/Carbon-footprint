-- models/marts/mart_co2_intensity.sql
-- ─────────────────────────────────────────────────────────────
-- KPI: CO2 intensity per tonne of cement (Scope 1+2+3)
-- ใช้โดย Streamlit dashboard และ Grafana
-- ─────────────────────────────────────────────────────────────
{{ config(materialized='table') }}

WITH scope1 AS (SELECT * FROM {{ ref('int_scope1_emissions') }}),
     scope2 AS (SELECT * FROM {{ ref('int_scope2_emissions') }}),
     scope3 AS (SELECT * FROM {{ ref('int_scope3_emissions') }})

SELECT
    s1.reading_date,
    s1.plant_id,

    -- ─── Scope 1: direct ────────────────────────────────────
    ROUND(s1.scope1_intensity_avg::numeric, 2)      AS scope1_kg_per_t,
    ROUND(s1.scope1_rolling_7d::numeric,  2)        AS scope1_7d_avg,
    ROUND(s1.scope1_rolling_30d::numeric, 2)        AS scope1_30d_avg,

    -- ─── Scope 2: electricity ───────────────────────────────
    ROUND(s2.scope2_intensity_kg_per_t::numeric, 2) AS scope2_kg_per_t,

    -- ─── Scope 3: value chain ───────────────────────────────
    ROUND(s3.scope3_total_kg_per_t::numeric, 2)     AS scope3_kg_per_t,

    -- ─── Total (Scope 1+2+3) ────────────────────────────────
    ROUND((
        s1.scope1_intensity_avg +
        s2.scope2_intensity_kg_per_t +
        s3.scope3_total_kg_per_t
    )::numeric, 2)                                  AS total_co2_kg_per_t,

    -- ─── Data quality ───────────────────────────────────────
    s1.reading_count,
    s1.anomaly_count,
    s1.exceeds_2030_target,

    -- ─── Metadata ───────────────────────────────────────────
    CURRENT_TIMESTAMP                               AS updated_at

FROM scope1 s1
LEFT JOIN scope2 s2 USING (reading_date, plant_id)
LEFT JOIN scope3 s3 USING (reading_date, plant_id)
ORDER BY reading_date DESC