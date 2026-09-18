import os
import io
import warnings

import pandas as pd
import psycopg2

from dotenv import load_dotenv
from google.cloud import storage


warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable"
)


load_dotenv()


# =========================
# CONFIG
# =========================

PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
GCS_PATH = os.getenv("GCS_RAW_BATCH_PATH")

EXTRACT_SQL_PATH = os.getenv("POSTGRES_EXTRACT_SQL_PATH")
DATE_SQL_PATH = os.getenv("POSTGRES_EXTRACT_DATE_SQL_PATH")

START_DATE = os.getenv("BATCH_START_DATE")
END_DATE = os.getenv("BATCH_END_DATE")


# =========================
# LOAD SQL
# =========================

with open(EXTRACT_SQL_PATH, "r") as f:
    EXTRACT_QUERY = f.read()

with open(DATE_SQL_PATH, "r") as f:
    DATE_QUERY = f.read()


# =========================
# POSTGRES CONNECTION
# =========================

def get_connection():

    return psycopg2.connect(
        host=os.getenv("POSTGRES_SOURCE_HOST"),
        port=os.getenv("POSTGRES_SOURCE_PORT"),
        dbname=os.getenv("POSTGRES_SOURCE_DB"),
        user=os.getenv("POSTGRES_SOURCE_USER"),
        password=os.getenv("POSTGRES_SOURCE_PASSWORD")
    )


# =========================
# UPLOAD TO GCS
# =========================

def upload_day(bucket, df, trip_date):

    trip_date = pd.Timestamp(trip_date)

    destination = (
        f"{GCS_PATH}/"
        f"year={trip_date.year}/"
        f"month={trip_date.month:02d}/"
        f"day={trip_date.day:02d}/"
        f"trips.csv"
    )

    buffer = io.StringIO()

    df.to_csv(
        buffer,
        index=False
    )

    bucket.blob(destination).upload_from_string(
        buffer.getvalue(),
        content_type="text/csv"
    )

    print(
        f"Uploaded {trip_date.date()} "
        f"({len(df)} rows)"
    )


# =========================
# MAIN
# =========================

def main():

    conn = get_connection()

    client = storage.Client(
        project=PROJECT_ID
    )

    bucket = client.bucket(
        BUCKET_NAME
    )

    print("Getting available trip dates...")

    dates = pd.read_sql(
        DATE_QUERY,
        conn,
        params=[
            START_DATE,
            END_DATE
        ]
    )["trip_date"]

    print(f"Total days: {len(dates)}")

    for trip_date in dates:

        trip_date = pd.Timestamp(trip_date)

        next_day = (
            trip_date
            + pd.Timedelta(days=1)
        )

        print(
            f"Extracting {trip_date.date()}"
        )

        df = pd.read_sql(
            EXTRACT_QUERY,
            conn,
            params=[
                trip_date,
                next_day
            ]
        )

        if df.empty:
            print(
                f"No data: {trip_date.date()}"
            )
            continue

        df["trip_date"] = trip_date.date()

        upload_day(
            bucket,
            df,
            trip_date
        )

    conn.close()

    print("PostgreSQL to GCS completed")


if __name__ == "__main__":
    main()