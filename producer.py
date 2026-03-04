import sys
import requests
import json
import time
import logging
from confluent_kafka import Producer

logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def error_gb(err):
        logging.error(f'Global Error : {err}')

def delivery_report(err, msg):
    if err is not None:
        if err.retriable():
            logging.error(f"Retriable error: {err}")
        elif err.fatal():
            logging.critical(f"Fatal error, shutting down: {err}")
        else:
            logging.error(f"Delivery failed: {err}")
    else:
        logging.info(
            f"Success, message delivered to {msg.topic()}[{msg.partition()}]"
        )

conf = {'bootstrap.servers': 'localhost:9092'}

try:
    producer = Producer(conf)
except Exception as e:
    logging.error(f'Failed to create Producer : {e}')
    sys.exit(1)

OPENSKY_URL = "https://opensky-network.org/api/states/all"

while True:
    try:
        r = requests.get(OPENSKY_URL, timeout=10)
        r.raise_for_status()

        data = r.json()
        states = data.get("states", [])
        logging.info(f"Fetched {len(states)} flights")

    except requests.exceptions.HTTPError as errh:
        logging.error("HTTP error returned from API")
        time.sleep(10)
        continue
    except requests.exceptions.ReadTimeout as errrt:
        logging.error("API request timed out")
        time.sleep(10)
        continue
    except requests.exceptions.ConnectionError as conerr:
        logging.error("Failed to establish connection to API")
        time.sleep(10)
        continue
    except requests.exceptions.RequestException as errex:
        logging.error("Unexpected request exception occurred")
        time.sleep(10)
        continue

    for state in states:
        try:
            flight_json = json.dumps(state).encode("utf-8")
            producer.produce('flight_positions', flight_json, on_delivery=delivery_report)
        except BufferError as e:
            logging.info(f'Local producer queue is full,{e}')
            producer.poll(1)

    producer.flush()
    logging.info("Batch sent to Kafka\n")

    time.sleep(10)