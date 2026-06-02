from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "airflow",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
}

# ── Task callables defined OUTSIDE the DAG context (best practice) ────────────

def task_produce(ti):
    from etl.kafka_producer import produce_jobs
    count = produce_jobs(pages=5)
    if count is None or count == 0:
        raise ValueError("Producer returned 0 jobs — check API connectivity")
    ti.xcom_push(key="job_count", value=count)
    print(f"✅ Produced {count} jobs to Kafka")


def task_consume(ti):
    from etl.kafka_consumer import consume_jobs
    raw_path = consume_jobs(timeout_ms=15000)
    if not raw_path:
        raise ValueError("consume_jobs() returned None — consumer may have timed out")
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw file not created at: {raw_path}")
    ti.xcom_push(key="raw_path", value=raw_path)
    print(f"✅ Consumed → {raw_path}")


def task_spark_transform(ti):
    raw_path = ti.xcom_pull(task_ids="consume", key="raw_path")
    if not raw_path:
        raise ValueError("raw_path XCom is None — did the consume task succeed?")
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw file missing at: {raw_path}")

    # Always reload so DAG reloads pick up code changes without restarting
    sys.path.insert(0, "/opt/airflow")
    import importlib
    import spark_jobs.spark_transform as st
    importlib.reload(st)

    cleaned_path = st.run(raw_path=raw_path)   # ← calls run(), not clean_jobs_spark()

    if not cleaned_path or not os.path.exists(cleaned_path):
        raise FileNotFoundError(f"Spark output missing at: {cleaned_path}")

    ti.xcom_push(key="cleaned_path", value=cleaned_path)
    print(f"✅ Spark transform done → {cleaned_path}")


def task_load(ti):
    from etl.load import load
    cleaned_path = ti.xcom_pull(task_ids="spark_transform", key="cleaned_path")
    if not cleaned_path:
        raise ValueError("cleaned_path XCom is None — did spark_transform succeed?")
    if not os.path.exists(cleaned_path):
        raise FileNotFoundError(f"Cleaned file missing at: {cleaned_path}")
    load(cleaned_path)
    print("✅ Load to Postgres complete")


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="cambodia_jobs_pipeline",
    default_args=default_args,
    description="API -> Kafka -> Spark (cluster) -> Postgres",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["jobs", "etl", "kafka", "spark"],
) as dag:

    t1 = PythonOperator(task_id="produce",         python_callable=task_produce)
    t2 = PythonOperator(task_id="consume",         python_callable=task_consume)
    t3 = PythonOperator(task_id="spark_transform", python_callable=task_spark_transform)
    t4 = PythonOperator(task_id="load",            python_callable=task_load)

    t1 >> t2 >> t3 >> t4