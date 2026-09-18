import os
import io
import sys
import json
import time
import random
import requests

from datetime import datetime,timezone
from dotenv import load_dotenv
from fastavro import schemaless_writer
from google.cloud import pubsub_v1

load_dotenv()

# menagmbil konfigurasi

PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_NAME = os.getenv("PUBSUB_INPUT_TOPIC")
STATUS_URL = os.getenv("STATION_STATUS_URL")
INFO_URL = os.getenv("STATION_INFORMATION_URL")
INTERVAL = int(os.getenv("STREAM_INTERVAL"))

# untuk membuka file avro dan membaca nya
with open(os.getenv("AVRO_SCHEMA_PATH")) as f:
    AVRO_SCHEMA = json.load(f)

# membuat client untuk memngirim data ke pubsub
publisher = pubsub_v1.PublisherClient()

# membuat TOPIC_PATH
TOPIC_PATH = publisher.topic_path(PROJECT_ID,TOPIC_NAME)

# function untuk mengambil data dari API
def get_stations(url):
    response = requests.get(url,timeout=30)
    response.raise_for_status()
    return response.json()["data"]["stations"] # mengambil key dari respones.json

# untuk menangkap nilai argument dari command line 
def get_arg(name):
    if name in sys.argv:
        pos = sys.argv.index(name) #mencari posisi index argumen
        return int(sys.argv[pos+1]) #mengambil nilai argumen 
    return None

# menggabungkan data status dan info menjadi satu event
def create_event(status,info):
    return{
        "station_id": str(status["station_id"]),
        "station_name": info.get("name"),
        "lat":info.get("lat"),
        "lon":info.get("lon"),
        "capacity":info.get("capacity"),
        "num_bikes_available": int(status.get("num_bikes_available")or 0),
        "num_docks_available":int(status.get("num_docks_available")or 0),
        "num_ebikes_available":int(status.get("num_ebikes_available")or 0),
        "is_installed":bool(status.get("is_installed",False)),
        "is_renting":bool(status.get("is_renting",False)),
        "is_returning":bool(status.get("is_returning",False)),
        "event_timestamp":datetime.now(timezone.utc)
    }

# membuat data event invalid
def make_invalid(event):
    field = random.choice(["station_id","num_bikes_available","num_docks_available"])
    if field == "station_id":
        event[field] = ""
    else:
        event[field] = -1

    print(f'Inject Data Invalid: {field}')

    return event

# membuat data anomaly
def make_anomaly(event):
    field = ("num_bikes_available")

    event[field] = 0

    print(f"Inject Data Anomaly: {field}")

    return event

# mengubah event menjadi schema Avro lalu dikrim ke pubsub
def publish(event):
    buffer = io.BytesIO()

    # untuk mengencode event menjadi binary sesuai skema avro ke buffer
    schemaless_writer(buffer,AVRO_SCHEMA,event)

    # untuk mengirim isi buffer ke pubsub 
    message_id = publisher.publish(TOPIC_PATH, buffer.getvalue()).result()

    print(
        f"Published "
        f"Station={event['station_id']} "
        f"name={event['station_name']} "
        f"id={message_id}"
    )

def main():
    invalid_every = get_arg("--invalid-every")
    anomaly_every = get_arg("--anomaly-every")

    print("Starting Citi Bike Streaming")

    # memanggil function get_station untuk API info
    information = get_stations(INFO_URL)

    # untuk mengubah list menjadi dictionary dengan key station_id
    station_info = {
        str(s["station_id"]) : s
        for s in information
    }

    print(f"Station Information Loaded: {len(station_info)}")

    while True:
        try:
            # Ambil data status dari API station status
            statuses = get_stations(STATUS_URL)

            # untuk mengetahui jumlah station
            print(f"Station Status Received : {len(statuses)}")

            # untuk mengambil data di statuses 
            for i, status in enumerate(statuses, start=1):
                station_id = str(status["station_id"])
                info = station_info.get(station_id, {})

                # membuat event dari status dan info
                event = create_event(status,info)

                # membuat data invalid-every dan anomaly-every
                if invalid_every and i % invalid_every == 0:
                    event = make_invalid(event)
                elif anomaly_every and i % anomaly_every == 0:
                    event = make_anomaly(event)

                # mengirim data ke pubsub
                publish(event)

            print(f"Waiting {INTERVAL} seconds")
            time.sleep(INTERVAL)

        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
