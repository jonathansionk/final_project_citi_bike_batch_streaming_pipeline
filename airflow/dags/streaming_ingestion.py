import os
import subprocess

import pendulum

from dotenv import load_dotenv

from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.utils.email import send_email

from google.cloud import bigquery
from google.api_core.exceptions import NotFound


# =========================================
# CONFIG
# =========================================

PROJECT_PATH = "/opt/airflow/project"

load_dotenv(f"{PROJECT_PATH}/.env")

PROJECT_ID = os.getenv("PROJECT_ID")
STAGING_DATASET = os.getenv(
    "BQ_STAGING_DATASET"
)


# =========================================
# FAILURE ALERT
# =========================================

def failure_alert(context):

    ti = context["task_instance"]

    send_email(
        to=os.getenv("AIRFLOW_ADMIN_EMAIL"),
        subject=f"[AIRFLOW FAIL] {ti.dag_id} - {ti.task_id}",
        html_content=f"""
        <h3>Citi Bike Streaming Failed</h3>
        DAG: {ti.dag_id}<br>
        Task: {ti.task_id}<br>
        Run ID: {context.get("run_id")}
        """
    )


# =========================================
# CREATE STAGING DATASET
# =========================================

@task
def create_staging_dataset():

    client = bigquery.Client(
        project=PROJECT_ID
    )

    dataset_id = (
        f"{PROJECT_ID}."
        f"{STAGING_DATASET}"
    )

    try:
        client.get_dataset(dataset_id)

        raise AirflowSkipException(
            "Staging dataset already exists"
        )

    except NotFound:
        pass


    sql_path = os.path.join(
        PROJECT_PATH,
        "sql/staging/ddl/"
        "create_staging.sql"
    )

    with open(
        sql_path,
        "r",
        encoding="utf-8"
    ) as f:
        query = f.read()

    client.query(query).result()


# =========================================
# RUN STREAMING SCRIPT
# =========================================

@task
def run_script(script):

    subprocess.run(
        ["python", script],
        cwd=PROJECT_PATH,
        env=os.environ.copy(),
        check=True
    )


# =========================================
# DAG
# =========================================

@dag(
    dag_id="citibike_streaming_ingestion",

    start_date=pendulum.datetime(
        2026,
        9,
        15,
        tz="Asia/Jakarta"
    ),

    # dijalankan manual
    schedule=None,

    catchup=False,

    default_args={
        "owner": "jonathan",

        "trigger_rule": "none_failed",

        "on_failure_callback":
            failure_alert
    },

    tags=[
        "citibike",
        "streaming",
        "ingestion"
    ]
)
def streaming_ingestion():


    # =====================================
    # 1. CREATE STAGING
    # =====================================

    staging = create_staging_dataset.override(
        task_id="create_staging_dataset"
    )()


    # =====================================
    # 2. PRODUCER
    # =====================================

    producer = run_script.override(
        task_id="run_producer"
    )(
        "scripts/streaming/"
        "producer.py"
    )


    # =====================================
    # 3. DATAFLOW STREAMING
    # =====================================

    load_stream = run_script.override(
        task_id="load_streaming_bq"
    )(
        "scripts/streaming/"
        "load_streaming_bq.py"
    )


    # producer + Dataflow parallel
    staging >> [
        producer,
        load_stream
    ]


streaming_ingestion()