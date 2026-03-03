import sys
import requests
import json
import time
from confluent_kafka import Producer

def error_gb(err):
        print(f'Global Error : {err}')

def delivery_report(err,msg):
    if err.retriable():
        print(f'Retriable error : {err}')
    elif err.fatal():
        print(f'Fatal error, shutting down : {err}')
    else:
        print(f'Success, message delivered to {msg.topic()}[{msg.partition()}]')

conf = {'bootstrap.servers': 'localhost:9092'}

try:
    producer = Producer(conf)
except Exception as e:
    print(f'Failed to create Producer : {e}')
    sys.exit(1)

OPENSKY_URL = "https://opensky-network.org/api/states/all"

while True:
    try:
        r = requests.get(OPENSKY_URL, timeout=10)
        r.raise_for_status()  # <-- this makes HTTPError work

        data = r.json()
        states = data.get("states", [])
        print(f"Fetched {len(states)} flights")

    except requests.exceptions.HTTPError:
        print("HTTP error returned from API")
        time.sleep(10)
        continue

    except requests.exceptions.ReadTimeout:
        print("API request timed out")
        time.sleep(10)
        continue

    except requests.exceptions.ConnectionError:
        print("Failed to establish connection to API")
        time.sleep(10)
        continue

    except requests.exceptions.RequestException:
        print("Unexpected request exception occurred")
        time.sleep(10)
        continue

    for state in states:
        try:
            flight_json = json.dumps(state).encode("utf-8")
            producer.produce('flight_positions', flight_json, on_delivery=delivery_report)
        except BufferError as e:
            print(f'Local producer queue is full,{e}')
            producer.poll(1)

    producer.flush()
    print("Batch sent to Kafka\n")

    time.sleep(10)