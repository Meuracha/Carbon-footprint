-- models/marts/mart_benchmark_gap.sql
-- ─────────────────────────────────────────────────────────────
-- KPI: Gap vs IEA global/ASEAN/Thailand benchmarks
-- แสดงว่า SCG อยู่ที่ไหนเทียบกับ industry
-- ─────────────────────────────────────────────────────────────
{{ config(materialized='table') }}

WITH co2 AS (
    SELECT * FROM {{ ref('mart_co2_intensity') }}
),
-- ใช้ year 2022 ซึ่งมีข้อมูลครบทั้ง Global และ ASEAN
benchmarks AS (
    SELECT * FROM {{ ref('stg_iea_benchmarks') }}
    WHERE year = 2022
),
global_bm AS (SELECT * FROM benchmarks WHERE region = 'Global'),
asean_bm  AS (SELECT * FROM benchmarks WHERE region = 'ASEAN'),
scg_target AS (
    SELECT * FROM {{ ref('stg_iea_benchmarks') }}
    WHERE region = 'SCG' AND year = 2030
)

SELECT
    c.reading_date,
    c.plant_id,
    c.scope1_kg_per_t                           AS scg_intensity,

    -- ─── vs Global benchmark ────────────────────────────────
    g.co2_intensity_kg_per_t                    AS global_avg,
    ROUND(
        (c.scope1_kg_per_t - g.co2_intensity_kg_per_t)::numeric, 2
    )                                           AS gap_vs_global,
    CASE
        WHEN c.scope1_kg_per_t < g.co2_intensity_kg_per_t
            THEN 'better_than_global'
        ELSE 'worse_than_global'
    END                                         AS global_status,

    -- ─── vs ASEAN benchmark ─────────────────────────────────
    a.co2_intensity_kg_per_t                    AS asean_avg,
    ROUND(
        (c.scope1_kg_per_t - a.co2_intensity_kg_per_t)::numeric, 2
    )                                           AS gap_vs_asean,

    -- ─── vs SCG 2030 target ──────────────────────────────────
    s.co2_intensity_kg_per_t                    AS scg_2030_target,
    ROUND(
        (c.scope1_kg_per_t - s.co2_intensity_kg_per_t)::numeric, 2
    )                                           AS gap_vs_scg_target,

    -- ─── percentile position ────────────────────────────────
    CASE
        WHEN c.scope1_kg_per_t < g.co2_intensity_kg_per_t * 0.9
            THEN 'top_10pct'
        WHEN c.scope1_kg_per_t < g.co2_intensity_kg_per_t
            THEN 'above_average'
        WHEN c.scope1_kg_per_t < g.co2_intensity_kg_per_t * 1.1
            THEN 'below_average'
        ELSE 'bottom_10pct'
    END                                         AS industry_position,

    c.updated_at

FROM co2 c
CROSS JOIN global_bm g
CROSS JOIN asean_bm a
CROSS JOIN scg_target s
ORDER BY c.reading_date DESC