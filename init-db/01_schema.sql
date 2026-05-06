-- ─────────────────────────────────────────────────────────────
--  Carbon Footprint Platform — PostgreSQL Schema
--  รันอัตโนมัติตอน container เริ่มครั้งแรก
-- ─────────────────────────────────────────────────────────────

-- ─── RAW TABLES (Airflow เขียน) ──────────────────────────────

-- 1. Sensor readings จาก kiln pipeline (Scope 1 sources)
CREATE TABLE IF NOT EXISTS raw_sensor_readings (
    id              BIGSERIAL PRIMARY KEY,
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    reading_date    DATE        NOT NULL,
    plant_id        TEXT        NOT NULL,
    sensor          TEXT        NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    unit_label      TEXT,
    is_anomaly      BOOLEAN     DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_sensor_date
    ON raw_sensor_readings (plant_id, sensor, reading_date DESC);

-- 2. Our World in Data — CO2 emissions
CREATE TABLE IF NOT EXISTS raw_owid_emissions (
    id              BIGSERIAL PRIMARY KEY,
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    year            INT         NOT NULL,
    country         TEXT        NOT NULL,
    co2_per_capita  DOUBLE PRECISION,
    co2_total_mt    DOUBLE PRECISION,
    cement_co2      DOUBLE PRECISION,     -- MT CO2 จากปูนซีเมนต์
    energy_co2      DOUBLE PRECISION
);

-- 3. IEA benchmark data
CREATE TABLE IF NOT EXISTS raw_iea_benchmarks (
    id              BIGSERIAL PRIMARY KEY,
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    year            INT         NOT NULL,
    sector          TEXT        NOT NULL,
    region          TEXT,
    metric          TEXT        NOT NULL,
    value           DOUBLE PRECISION,
    unit            TEXT
);

-- 4. OpenAQ emission readings
CREATE TABLE IF NOT EXISTS raw_openaq_readings (
    id              BIGSERIAL PRIMARY KEY,
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    measurement_date DATE        NOT NULL,
    location        TEXT,
    country         TEXT,
    parameter       TEXT        NOT NULL,  -- NOx, PM2.5, SO2
    value           DOUBLE PRECISION,
    unit            TEXT
);

-- ─── PIPELINE LOG ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_log (
    id          BIGSERIAL PRIMARY KEY,
    run_at      TIMESTAMPTZ DEFAULT NOW(),
    dag_id      TEXT        NOT NULL,
    source      TEXT        NOT NULL,
    rows_loaded INT         DEFAULT 0,
    status      TEXT        NOT NULL,      -- success / failed
    message     TEXT
);

-- ─── VERIFY ───────────────────────────────────────────────────
SELECT 'Carbon Footprint Platform schema ready' AS status;