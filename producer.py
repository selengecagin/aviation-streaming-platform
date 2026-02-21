import requests
import json
import time
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

OPENSKY_URL = "https://opensky-network.org/api/states/all"

while True:
    r = requests.get(OPENSKY_URL, timeout=10)
    data = r.json()
    states = data.get("states", [])

    print(f"Fetched {len(states)} flights")

    for state in states:
        flight_json = json.dumps(state).encode("utf-8")
        producer.produce('flight_positions', flight_json)
        producer.poll(0)

    producer.flush()
    print("Batch sent to Kafka\n")

    time.sleep(10)