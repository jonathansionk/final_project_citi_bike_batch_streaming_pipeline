import os
import csv
import io

import apache_beam as beam
from dotenv import load_dotenv
from apache_beam.options.pipeline_options import PipelineOptions

load_dotenv()

# mengambil konfigurasi environment
PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("GCP_REGION")
BUCKET = os.getenv("BUCKET_NAME")
DATASET = os.getenv("BQ_STAGING_DATASET")
TABLE = os.getenv("BQ_BATCH_STAGING_TABLE")

# lokasi file di GCS
GCS_RAW_BATCH_PATH = os.getenv("GCS_RAW_BATCH_PATH")

GCS_INPUT = (
    f"gs://{BUCKET}/"
    f"{GCS_RAW_BATCH_PATH}/"
    f"year=*/month=*/day=*/trips.csv"
)

# alamat tabel tujuan di Big Query
BQ_TABLE = (f'{PROJECT_ID}:{DATASET}.{TABLE}')

def parse_csv(line,header):

    # untuk membaca data per baris file csv
    values = next(csv.reader(io.StringIO(line)))

    # memasangkan value dengan header menjadi dictionary
    data = dict(zip(header,values))

    return data

def run():

    # konfigurasi job di dataflow
    options = PipelineOptions(
        project=PROJECT_ID,
        region=REGION,
        runner="DataflowRunner",
        temp_location=f"gs://{BUCKET}/temp/batch",
        staging_location=f'gs://{BUCKET}/staging/batch',
        job_name=("jcdeah-009-jonathan-citibike-batch"),
        num_workers=1,
        max_num_workers=1,
        save_main_session=True
    )

    # membuat pipeline apache beam sesuai konfigurasi options
    with beam.Pipeline(options=options) as p:

        lines = (
            p # object pipeline yang di buat
            | "Read.CSV" # label step di pipeline
            >> beam.io.ReadFromText( #membaca file text dari GCS_INPUT
                GCS_INPUT,
                skip_header_lines=1
            )
        )

        # membuat list header 
        header = [
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
            "member_casual",
            "trip_date"
        ]

        rows = (
            lines
            | "parse_csv"
            >> beam.Map(parse_csv,header=header) #mengambil data per baris dari lines kemudian di masukan ke function parse_csv
        )

        # untuk memindahakn data ke bigquery
        rows | "Write BigQuery" >> beam.io.WriteToBigQuery( 
            BQ_TABLE, # alamat tabel tujuan 

            # skema tabel dari header
            schema=",".join(
                f"{column}:string"
                for column in header
            ),

            # untuk me replace data saat di jalankan berulang
            write_disposition="WRITE_TRUNCATE",

            # jika tabel tidak ada, maka buat tabel dari BQ_TABEL
            create_disposition="CREATE_IF_NEEDED"
        )

if __name__ == "__main__":
    run()
