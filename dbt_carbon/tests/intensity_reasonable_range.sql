-- tests/intensity_reasonable_range.sql
-- ─────────────────────────────────────────────────────────────
-- Custom test: CO2 intensity ต้องอยู่ในช่วงที่สมเหตุสมผล
-- Cement industry: 200-1200 kg CO2/t-cement
-- ถ้าเกินนี้ = sensor error หรือ calculation bug
-- ─────────────────────────────────────────────────────────────
SELECT
    reading_date,
    plant_id,
    scope1_kg_per_t,
    'out_of_reasonable_range'   AS issue
FROM {{ ref('mart_co2_intensity') }}
WHERE
    scope1_kg_per_t < 200
    OR scope1_kg_per_t > 1200