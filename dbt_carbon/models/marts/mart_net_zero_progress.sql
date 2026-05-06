-- models/marts/mart_net_zero_progress.sql
-- ─────────────────────────────────────────────────────────────
-- KPI: Progress toward SCG Net Zero 2050
-- เปรียบเทียบ current intensity vs targets
-- ─────────────────────────────────────────────────────────────
{{ config(materialized='table') }}

WITH co2 AS (
    SELECT * FROM {{ ref('mart_co2_intensity') }}
),
progress AS (
    SELECT
        reading_date,
        plant_id,
        scope1_kg_per_t                         AS current_intensity,

        -- ─── SCG targets ────────────────────────────────────
        {{ var('net_zero_baseline_intensity') }} AS baseline_2020,
        {{ var('net_zero_target_2030') }}        AS target_2030,
        {{ var('net_zero_target_2050') }}        AS target_2050,

        -- ─── Gap vs targets ─────────────────────────────────
        scope1_kg_per_t - {{ var('net_zero_target_2030') }}
                                                AS gap_vs_2030_target,
        scope1_kg_per_t - {{ var('net_zero_target_2050') }}
                                                AS gap_vs_2050_target,

        -- ─── % progress toward 2030 target ──────────────────
        -- (baseline - current) / (baseline - target) * 100
        ROUND(
            GREATEST(0,
                ({{ var('net_zero_baseline_intensity') }} - scope1_kg_per_t) /
                NULLIF(
                    {{ var('net_zero_baseline_intensity') }} -
                    {{ var('net_zero_target_2030') }}, 0
                ) * 100
            )::numeric, 1
        )                                       AS progress_pct_to_2030,

        -- ─── % progress toward 2050 target ──────────────────
        ROUND(
            GREATEST(0,
                ({{ var('net_zero_baseline_intensity') }} - scope1_kg_per_t) /
                NULLIF(
                    {{ var('net_zero_baseline_intensity') }}, 0
                ) * 100
            )::numeric, 1
        )                                       AS progress_pct_to_2050,

        -- ─── Status ─────────────────────────────────────────
        CASE
            WHEN scope1_kg_per_t <= {{ var('net_zero_target_2030') }}
                THEN 'on_track'
            WHEN scope1_kg_per_t <= {{ var('net_zero_target_2030') }} * 1.05
                THEN 'near_target'
            ELSE 'off_track'
        END                                     AS status_2030,

        -- ─── Annual reduction needed ─────────────────────────
        -- Years remaining to 2050
        ROUND(
            (scope1_kg_per_t - {{ var('net_zero_target_2050') }}) /
            NULLIF(2050 - EXTRACT(YEAR FROM reading_date), 0)::numeric, 2
        )                                       AS required_annual_reduction,

        updated_at

    FROM co2
)
SELECT * FROM progress
ORDER BY reading_date DESC