import os
import io
import json

import apache_beam as beam

from dotenv import load_dotenv
from fastavro import schemaless_reader
from apache_beam.options.pipeline_options import PipelineOptions

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("GCP_REGION")
BUCKET = os.getenv("BUCKET_NAME")

SUBSCRIPTION = (
    f"projects/{PROJECT_ID}/subscriptions/"
    f"{os.getenv('PUBSUB_SUBSCRIPTION')}"
)

DATASET = os.getenv("BQ_STAGING_DATASET")

VALID_TABLE = (
    f"{PROJECT_ID}:{DATASET}."
    f"{os.getenv('BQ_STREAM_VALID_TABLE')}"
)

INVALID_TABLE = (
    f"{PROJECT_ID}:{DATASET}."
    f"{os.getenv('BQ_STREAM_INVALID_TABLE')}"
)

# membuka schema avro
with open(os.getenv("AVRO_SCHEMA_PATH")) as f:
    AVRO_SCHEMA = json.load(f)

# untuk decode avro menjadi dictioinary
def decode_avro(message):
    return schemaless_reader(io.BytesIO(message), AVRO_SCHEMA)

# untuk memisahkan data invalid ke errors
def validate(data):

    errors = []

    if not data.get("station_id"):
        errors.append("station_id is empty")

    if data["num_bikes_available"] < 0:
        errors.append("num_bikes_available is negative")

    if data["num_docks_available"] < 0:
        errors.append("num_docks_available is negative")

    return errors

# memisahkan data menjadi 2 jalur
class ValidateRecord(beam.DoFn):

    def process(self, data):

        # mengecek data dengan function validate
        errors = validate(data)

        # kalau tidak ada error record di keluarkan dari funciton process
        if not errors:
            yield data

        # kalau error maka buat record invalid
        else:
            yield beam.pvalue.TaggedOutput(
                "invalid",
                {
                    "station_id": data["station_id"],
                    "error_reason": ", ".join(errors),
                    "raw_payload": json.dumps(data, default=str),
                    "event_timestamp": data["event_timestamp"]

                }
            )

# membuka schema valid
with open(os.getenv("BQ_STREAM_VALID_SCHEMA_PATH")) as f:
    VALID_SCHEMA = json.load(f)

# membuka schmea invalid
with open(os.getenv("BQ_STREAM_INVALID_SCHEMA_PATH")) as f:
    INVALID_SCHEMA = json.load(f)

def run():

    # Konfigurasi apache beam dataflow
    options = PipelineOptions(
        project=PROJECT_ID,
        region=REGION,
        runner="DataflowRunner",
        streaming=True,
        temp_location=f"gs://{BUCKET}/temp",
        staging_location=f"gs://{BUCKET}/staging",
        job_name=os.getenv("DATAFLOW_JOB_NAME"),
        save_main_session = True
    )

    # membuat pipeline sesuai konfigurasi options 
    with beam.Pipeline(options=options) as p:

        result = (
            p
            | "Read Pubsub"
            >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Decode Avro"
            >> beam.Map(decode_avro)
            | "Validate"
            >> beam.ParDo(ValidateRecord()).with_outputs("invalid", main="valid")
        )

        # memasukan result valid ke tabel valid
        result.valid | "write valid" >> beam.io.WriteToBigQuery(
            VALID_TABLE,
            schema=VALID_SCHEMA,
            method="STREAMING_INSERTS",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND"

        )

        # memasukan result invalid ke tabel invalid
        result.invalid | "write invalid" >> beam.io.WriteToBigQuery(
            INVALID_TABLE,
            schema=INVALID_SCHEMA,
            method="STREAMING_INSERTS",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND"
        )

if __name__=="__main__":
    run()