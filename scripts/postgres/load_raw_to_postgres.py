import os
import glob

import pandas as pd
import psycopg2

from dotenv import load_dotenv
from psycopg2.extras import execute_values


load_dotenv()

INPUT_PATH = os.getenv("INPUT_DATA_PATH")
FILE_PATTERN = os.getenv("FILE_PATTERN", "*.csv")


conn = psycopg2.connect(
    host=os.getenv("POSTGRES_SOURCE_HOST"),
    port=os.getenv("POSTGRES_SOURCE_PORT"),
    dbname=os.getenv("POSTGRES_SOURCE_DB"),
    user=os.getenv("POSTGRES_SOURCE_USER"),
    password=os.getenv("POSTGRES_SOURCE_PASSWORD")
)


INSERT_QUERY = """
INSERT INTO raw_citibike_trips (
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual
)
VALUES %s
ON CONFLICT (ride_id) DO NOTHING; 
"""


def load_file(file_path):

    print(f"Loading: {file_path}")

    df = pd.read_csv(file_path)

    df = df.where(
        pd.notnull(df),
        None
    )

    rows = list(
        df[
            [
                "ride_id",
                "rideable_type",
                "started_at",
                "ended_at",
                "start_station_name",
                "start_station_id",
                "end_station_name",
                "end_station_id",
                "start_lat",
                "start_lng",
                "end_lat",
                "end_lng",
                "member_casual"
            ]
        ].itertuples(
            index=False,
            name=None
        )
    )

    with conn.cursor() as cursor:

        execute_values(
            cursor,
            INSERT_QUERY,
            rows
        )

    conn.commit()

    print(f"Finished: {len(rows)} rows")


def main():

    files = glob.glob(
        os.path.join(
            INPUT_PATH,
            FILE_PATTERN
        )
    )

    print(f"Files found: {len(files)}")

    for file in files:
        load_file(file)

    conn.close()

    print("Load to PostgreSQL completed")

if __name__ == "__main__":
    main()