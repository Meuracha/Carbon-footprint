"""
dags/dag_ingest_sensors.py
───────────────────────────
DAG: ingest_sensor_readings
Schedule: ทุกวัน 18:00 น. (หลังตลาดปิด / shift เย็นของโรงงาน)

ดึงข้อมูล sensor readings จาก kiln pipeline database
แล้ว load ลง carbon platform PostgreSQL
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import psycopg2.extras
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ─── Config ───────────────────────────────────────────────────
CARBON_DB = {
    "host":     "postgres-carbon",
    "dbname":   "carbondb",
    "user":     os.environ["CARBON_DB_USER"],
    "password": os.environ["CARBON_DB_PASS"],
    "port":     5432,
}

# Sensors ที่เกี่ยวกับ carbon footprint
CARBON_SENSORS = [
    "CO2_emission_intensity",
    "specific_heat_consumption",
    "coal_consumption_rate",
    "NOx_emission",
    "dust_emission",
    "SO2_emission",
    "O2_flue_gas",
]

PLANT_ID = "SCG_RAYONG_01"


def generate_mock_sensor_data(execution_date: datetime) -> list[dict]:
    """
    จำลองข้อมูล sensor รายวัน
    ในโปรเจคจริง: ดึงจาก TimescaleDB ของ kiln pipeline แต่ตอนนี้ไม่ได้ดึง
    """
    import random
    import math

    records = []
    t = (execution_date - datetime(2024, 1, 1, tzinfo=execution_date.tzinfo)).days

    specs = {
        "CO2_emission_intensity":    {"base": 680, "std": 12,  "unit": "kg_CO2_per_t_cement"},
        "specific_heat_consumption": {"base": 750, "std": 15,  "unit": "kcal_per_kg_clinker"},
        "coal_consumption_rate":     {"base": 10.2,"std": 0.4, "unit": "t_per_hr"},
        "NOx_emission":              {"base": 380, "std": 35,  "unit": "mg_per_Nm3"},
        "dust_emission":             {"base": 12,  "std": 2.5, "unit": "mg_per_Nm3"},
        "SO2_emission":              {"base": 45,  "std": 8,   "unit": "mg_per_Nm3"},
        "O2_flue_gas":               {"base": 2.5, "std": 0.3, "unit": "pct"},
    }

    thresholds = {
        "CO2_emission_intensity":    {"lo": 600, "hi": 800},
        "specific_heat_consumption": {"lo": 680, "hi": 820},
        "coal_consumption_rate":     {"lo": 5,   "hi": 15},
        "NOx_emission":              {"lo": 0,   "hi": 800},
        "dust_emission":             {"lo": 0,   "hi": 20},
        "SO2_emission":              {"lo": 0,   "hi": 200},
        "O2_flue_gas":               {"lo": 1.0, "hi": 5.0},
    }

    for sensor, spec in specs.items():
        drift = spec["std"] * 0.4 * math.sin(2 * math.pi * t / 365)
        value = round(spec["base"] + drift + random.gauss(0, spec["std"] * 0.3), 3)
        lo = thresholds[sensor]["lo"]
        hi = thresholds[sensor]["hi"]
        records.append({
            "reading_date": execution_date.date(),
            "plant_id":     PLANT_ID,
            "sensor":       sensor,
            "value":        value,
            "unit_label":   spec["unit"],
            "is_anomaly":   value < lo or value > hi,
        })

    return records


def ingest_sensor_readings(**context):
    execution_date = context["logical_date"]
    print(f"Ingesting sensor data for {execution_date.date()}")

    records = generate_mock_sensor_data(execution_date)

    conn = psycopg2.connect(**CARBON_DB)
    cur  = conn.cursor()

    # ลบข้อมูลเดิมของวันนี้ก่อน (idempotent)
    cur.execute("""
        DELETE FROM raw_sensor_readings
        WHERE reading_date = %s AND plant_id = %s
    """, (execution_date.date(), PLANT_ID))

    # Insert ใหม่
    psycopg2.extras.execute_values(cur, """
        INSERT INTO raw_sensor_readings
            (reading_date, plant_id, sensor, value, unit_label, is_anomaly)
        VALUES %s
    """, [(r["reading_date"], r["plant_id"], r["sensor"],
           r["value"], r["unit_label"], r["is_anomaly"])
          for r in records])

    # Log
    cur.execute("""
        INSERT INTO pipeline_log (dag_id, source, rows_loaded, status)
        VALUES (%s, %s, %s, %s)
    """, ("ingest_sensor_readings", "kiln_simulator",
          len(records), "success"))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(records)} sensor records for {execution_date.date()}")
    return len(records)


# ─── DAG definition ───────────────────────────────────────────
with DAG(
    dag_id="ingest_sensor_readings",
    description="ดึง sensor data จาก kiln pipeline รายวัน",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 18 * * 1-5",    # 18:00 จันทร์-ศุกร์
    catchup=False,                       # ไม่ backfill ย้อนหลัง
    max_active_runs=1,
    default_args={
        "owner":            "scg-data-team",
        "retries":          2,
        "retry_delay":      timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["carbon", "sensors", "ingestion"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_sensor_readings",
        python_callable=ingest_sensor_readings,
    )
    
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_run",
        trigger_dag_id="dbt_run_carbon",
        # wait_forcompletion=False,
        reset_dag_run=True,
    )
    
    ingest >> trigger_dbt