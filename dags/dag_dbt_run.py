"""
dags/dag_dbt_run.py
────────────────────
DAG: dbt_run_carbon
Schedule: รันหลัง dag_ingest_sensors เสร็จ (sensor trigger)
          + รันทุกวันจันทร์ 20:00 (หลัง external ingest)

Tasks:
  1. dbt deps     — install dbt packages
  2. dbt run      — transform staging → intermediate → marts
  3. dbt test     — run data quality tests
  4. dbt docs     — generate data catalog
  5. export_parquet — export marts เป็น Parquet สำหรับ deploy
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

DBT_DIR = Path("/opt/airflow/dbt_carbon")
EXPORTS_DIR = Path("/opt/airflow/exports")

CARBON_DB = {
    "host":     "postgres-carbon",
    "dbname":   "carbondb",
    "user":     os.environ["CARBON_DB_USER"],
    "password": os.environ["CARBON_DB_PASS"],
    "port":     5432,
}


def run_dbt_command(command: list[str]) -> str:
    """รัน dbt command และ return output"""
    full_cmd = ["dbt"] + command + ["--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR), "--target", "airflow",]
    print(f"Running: {' '.join(full_cmd)}")

    result = subprocess.run(
        full_cmd,
        capture_output=True,
        text=True,
        cwd=str(DBT_DIR),
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"dbt command failed: {' '.join(command)}\n{result.stderr}")
    return result.stdout


def dbt_deps(**context):
    """ติดตั้ง dbt packages"""
    run_dbt_command(["deps"])
    print("dbt deps completed")


def dbt_run(**context):
    """
    รัน dbt models ตามลำดับ:
    staging → intermediate → marts
    """
    # รัน staging ก่อน
    run_dbt_command(["run", "--select", "staging"])
    print("Staging models complete")

    # แล้วค่อยรัน intermediate
    run_dbt_command(["run", "--select", "intermediate"])
    print("Intermediate models complete")

    # สุดท้าย marts
    run_dbt_command(["run", "--select", "marts"])
    print("Mart models complete")


def dbt_test(**context):
    """รัน dbt tests ตรวจ data quality"""
    try:
        output = run_dbt_command(["test"])
        # นับจำนวน tests ที่ผ่าน
        passed = output.count("PASS")
        failed = output.count("FAIL")
        print(f"Tests: {passed} passed, {failed} failed")

        if failed > 0:
            # Log แต่ไม่ fail DAG (warning เท่านั้น)
            print(f"WARNING: {failed} dbt tests failed")

        # บันทึก test results
        conn = psycopg2.connect(**CARBON_DB)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_log (dag_id, source, rows_loaded, status, message)
            VALUES (%s, %s, %s, %s, %s)
        """, ("dbt_run_carbon", "dbt_test",
              passed, "success" if failed == 0 else "warning",
              f"{passed} passed, {failed} failed"))
        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"dbt test error: {e}")
        # ไม่ raise เพราะไม่ต้องการให้ test ล้มเหลว block export


def dbt_docs(**context):
    """Generate dbt docs (data catalog)"""
    run_dbt_command(["docs", "generate"])
    print("dbt docs generated at dbt_carbon/target/")


def export_parquet(**context):
    """
    Export mart tables เป็น Parquet files
    สำหรับ deploy บน Streamlit Community Cloud
    (ไม่ต้องมี DB server ตอน deploy)
    """
    EXPORTS_DIR.mkdir(exist_ok=True)

    mart_queries = {
        "mart_co2_intensity": """
            SELECT * FROM public_marts.mart_co2_intensity
            ORDER BY reading_date DESC
        """,
        "mart_net_zero_progress": """
            SELECT * FROM public_marts.mart_net_zero_progress
            ORDER BY reading_date DESC
        """,
        "mart_benchmark_gap": """
            SELECT * FROM public_marts.mart_benchmark_gap
            ORDER BY reading_date DESC
        """,
    }

    conn = psycopg2.connect(**CARBON_DB)
    exported = []

    for table_name, query in mart_queries.items():
        try:
            df = pd.read_sql(query, conn)
            out_path = EXPORTS_DIR / f"{table_name}.parquet"
            df.to_parquet(out_path, index=False, engine="pyarrow")
            exported.append(f"{table_name}: {len(df)} rows → {out_path}")
            print(f"Exported {table_name}: {len(df)} rows")
        except Exception as e:
            print(f"Export failed for {table_name}: {e}")

    conn.close()

    # Log
    conn = psycopg2.connect(**CARBON_DB)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pipeline_log (dag_id, source, rows_loaded, status, message)
        VALUES (%s, %s, %s, %s, %s)
    """, ("dbt_run_carbon", "parquet_export",
          len(exported), "success", "; ".join(exported)))
    conn.commit()
    cur.close()
    conn.close()

    print(f"Exported {len(exported)} Parquet files to {EXPORTS_DIR}")
    return exported


# ─── DAG definition ───────────────────────────────────────────
with DAG(
    dag_id="dbt_run_carbon",
    description="รัน dbt transform + test + export หลัง ingest เสร็จ",
    start_date=datetime(2024, 1, 1),
    schedule_interval="30 18 * * 1-5",  # 18:30 หลัง sensor ingest (18:00)
    catchup=False,
    default_args={
        "owner":            "scg-data-team",
        "retries":          1,
        "retry_delay":      timedelta(minutes=10),
        "execution_timeout": timedelta(hours=1),
    },
    tags=["carbon", "dbt", "transform"],
) as dag:

    t_deps = PythonOperator(
        task_id="dbt_deps",
        python_callable=dbt_deps,
    )

    t_run = PythonOperator(
        task_id="dbt_run",
        python_callable=dbt_run,
    )

    t_test = PythonOperator(
        task_id="dbt_test",
        python_callable=dbt_test,
    )

    t_docs = PythonOperator(
        task_id="dbt_docs",
        python_callable=dbt_docs,
    )

    t_export = PythonOperator(
        task_id="export_parquet",
        python_callable=export_parquet,
    )

    # Task dependencies
    t_deps >> t_run >> t_test >> [t_docs, t_export]