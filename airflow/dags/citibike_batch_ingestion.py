import os
import subprocess
import psycopg2

from datetime import timedelta

import pendulum
from dotenv import load_dotenv

from airflow.sdk import dag, task
from airflow.utils.email import send_email
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


PROJECT_PATH = "/opt/airflow/project"
load_dotenv(f"{PROJECT_PATH}/.env")


def failure_alert(context):

    ti = context["task_instance"]

    send_email(
        to=os.getenv("AIRFLOW_ADMIN_EMAIL"),
        subject=f"[AIRFLOW FAIL] {ti.dag_id} - {ti.task_id}",
        html_content=f"""
        <h3>Citi Bike Batch Ingestion Failed</h3>
        DAG: {ti.dag_id}<br>
        Task: {ti.task_id}<br>
        Run ID: {context.get("run_id")}<br>
        Logical Date: {context.get("logical_date")}
        """
    )

@task
def check_postgres_source():

    # untuk test error
    # raise Exception("TEST ERROR - sengaja dibuat")

    conn = psycopg2.connect(
        host="postgres_source",
        port=5432,
        dbname=os.getenv("POSTGRES_SOURCE_DB"),
        user=os.getenv("POSTGRES_SOURCE_USER"),
        password=os.getenv("POSTGRES_SOURCE_PASSWORD")
    )

    path = os.path.join(
        PROJECT_PATH,
        "sql/postgres/quality/cek_raw_citibike.sql"
    )

    with open(path, encoding="utf-8") as f:
        query = f.read()

    with conn.cursor() as cursor:
        cursor.execute(query)
        total_rows = cursor.fetchone()[0]

    conn.close()

    print(f"PostgreSQL source rows: {total_rows}")

    if total_rows == 0:
        raise Exception("PostgreSQL source has no data")

@task
def extract_postgres_to_gcs():

    env = os.environ.copy()

    env["POSTGRES_SOURCE_HOST"] = "postgres_source"
    env["POSTGRES_SOURCE_PORT"] = "5432"

    subprocess.run(
        [
            "python",
            "scripts/batch/extract_postgres_to_gcs.py"
        ],
        cwd=PROJECT_PATH,
        env=env,
        check=True
    )


@dag(
    dag_id="citibike_batch_ingestion",

    start_date=pendulum.datetime(
        2026, 9, 15,
        tz="Asia/Jakarta"
    ),

    # menjalankan DAG manual
    # schedule=None,
    
    # setiap hari jam 00:00 WIB
    schedule="0 0 * * *",
    catchup=False,
    max_active_runs=1,

    default_args={
        "owner": "jonathan",
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": failure_alert
    },

    tags=["citibike", "batch", "ingestion"]
)
def batch_ingestion():

    check_source = check_postgres_source()

    extract = extract_postgres_to_gcs()

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_batch_transform",
        trigger_dag_id="citibike_batch_transform",
        wait_for_completion=False
    )

    check_source >> extract >> trigger_transform

batch_ingestion()