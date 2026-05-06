-- tests/co2_not_negative.sql
-- ─────────────────────────────────────────────────────────────
-- Custom test: CO2 intensity ต้องไม่ติดลบ
-- ถ้า query return rows = test FAIL
-- ─────────────────────────────────────────────────────────────
SELECT
    reading_date,
    plant_id,
    scope1_kg_per_t
FROM {{ ref('mart_co2_intensity') }}
WHERE scope1_kg_per_t < 0