import os
import subprocess

from datetime import timedelta

import pendulum
from dotenv import load_dotenv

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.utils.email import send_email

from google.cloud import bigquery
from google.api_core.exceptions import NotFound


# =========================================
# CONFIG
# =========================================

PROJECT_PATH = "/opt/airflow/project"
load_dotenv(f"{PROJECT_PATH}/.env")

PROJECT_ID = os.getenv("PROJECT_ID")

STAGING_DATASET = os.getenv("BQ_STAGING_DATASET")
SILVER_DATASET = os.getenv("BQ_SILVER_DATASET")
GOLD_DATASET = os.getenv("BQ_GOLD_DATASET")


# =========================================
# FAILURE ALERT
# =========================================

def failure_alert(context):

    ti = context["task_instance"]

    send_email(
        to=os.getenv("AIRFLOW_ADMIN_EMAIL"),
        subject=f"[AIRFLOW FAIL] {ti.dag_id} - {ti.task_id}",
        html_content=f"""
        <h3>Citi Bike Batch Transform Failed</h3>
        DAG: {ti.dag_id}<br>
        Task: {ti.task_id}<br>
        Run ID: {context.get("run_id")}
        """
    )


# =========================================
# TASK FUNCTION
# =========================================

@task
def run_python(script):

    # Untuk test alert error
    # if "load_batch_bq.py" in script:
    #     raise Exception("TEST ERROR - sengaja dibuat")

    subprocess.run(
        ["python", script],
        cwd=PROJECT_PATH,
        check=True
    )


@task
def run_sql(sql_file):

    # Untuk test alert error
    # if "transform_stream_silver.sql" in sql_file:
    #     raise Exception("TEST ERROR - sengaja dibuat")

    path = os.path.join(
        PROJECT_PATH,
        sql_file
    )

    with open(path, encoding="utf-8") as f:
        query = f.read()

    bigquery.Client(
        project=PROJECT_ID
    ).query(query).result()


@task
def create_dataset(dataset_name, sql_file):

    client = bigquery.Client(
        project=PROJECT_ID
    )

    dataset_id = f"{PROJECT_ID}.{dataset_name}"

    try:
        client.get_dataset(dataset_id)

        raise AirflowSkipException(
            f"{dataset_id} already exists"
        )

    except NotFound:
        pass

    path = os.path.join(
        PROJECT_PATH,
        sql_file
    )

    with open(path, encoding="utf-8") as f:
        query = f.read()

    client.query(query).result()


# =========================================
# DAG
# =========================================

@dag(
    dag_id="citibike_batch_transform",

    start_date=pendulum.datetime(
        2026,
        9,
        15,
        tz="Asia/Jakarta"
    ),

    # DAG ini dijalankan otomatis setalah DAG batch ingestion Succed
    schedule=None,

    catchup=False,
    max_active_runs=1,

    default_args={
        "owner": "jonathan",
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "trigger_rule": "none_failed",
        "on_failure_callback": failure_alert
    },

    tags=[
        "citibike",
        "batch",
        "transform"
    ]
)
def batch_transform():


    # =====================================
    # HELPER
    # =====================================

    def py(task_id, script):

        return run_python.override(
            task_id=task_id
        )(script)


    def sql(task_id, file):

        return run_sql.override(
            task_id=task_id
        )(file)


    def dataset(task_id, name, file):

        return create_dataset.override(
            task_id=task_id
        )(
            name,
            file
        )


    # =====================================
    # STAGING
    # =====================================

    create_staging = dataset(
        "create_staging_dataset",
        STAGING_DATASET,
        "sql/staging/ddl/create_staging.sql"
    )


    load_batch = py(
        "load_batch_bq",
        "scripts/batch/load_batch_bq.py"
    )


    batch_ref = sql(
        "create_dim_batch_station_reference",
        "sql/staging/dim/batch_station_reference.sql"
    )


    stream_ref = sql(
        "create_dim_stream_station_reference",
        "sql/staging/dim/stream_station_reference.sql"
    )


    check_staging = sql(
        "check_staging",
        "sql/staging/quality/cek_staging.sql"
    )


    # =====================================
    # SILVER
    # =====================================

    create_silver = dataset(
        "create_silver_dataset",
        SILVER_DATASET,
        "sql/silver/ddl/create_silver.sql"
    )


    station_mapping = sql(
        "create_dim_station_mapping",
        "sql/silver/dim/station_mapping.sql"
    )


    trip_clean = sql(
        "create_fact_citibike_trips_cleaned",
        "sql/silver/fact/transform_trip_silver.sql"
    )


    stream_clean = sql(
        "create_fact_valid_stream_cleaned",
        "sql/silver/fact/transform_stream_silver.sql"
    )


    check_silver = sql(
        "check_silver",
        "sql/silver/quality/cek_silver.sql"
    )


    # =====================================
    # GOLD
    # =====================================

    create_gold = dataset(
        "create_gold_dataset",
        GOLD_DATASET,
        "sql/gold/ddl/create_gold.sql"
    )


    marts = [

        sql(
            "mart_daily_trip_summary",
            "sql/gold/mart/daily_trip_summary.sql"
        ),

        sql(
            "mart_latest_station_status",
            "sql/gold/mart/latest_station_status.sql"
        ),

        sql(
            "mart_station_monitoring",
            "sql/gold/mart/station_monitoring.sql"
        ),

        sql(
            "mart_station_summary",
            "sql/gold/mart/station_summary.sql"
        ),

        sql(
            "mart_station_user_summary",
            "sql/gold/mart/station_user_summary.sql"
        ),

        sql(
            "mart_user_type_summary",
            "sql/gold/mart/user_type_summary.sql"
        )
    ]


    check_gold = sql(
        "check_gold",
        "sql/gold/quality/cek_gold.sql"
    )


    # =====================================
    # DEPENDENCIES
    # =====================================

    create_staging >> load_batch

    load_batch >> [
        batch_ref,
        stream_ref
    ] >> check_staging

    check_staging >> create_silver

    create_silver >> [
        station_mapping,
        trip_clean,
        stream_clean
    ] >> check_silver

    check_silver >> create_gold

    create_gold >> marts >> check_gold


batch_transform()