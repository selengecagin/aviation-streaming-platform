from confluent_kafka import Consumer, KafkaError
import json

def main():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "flight-consumer-group",
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe(["flight_positions"])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print("Error:", msg.error())
            else:
                value = msg.value().decode("utf-8")
                data = json.loads(value)

                states = data["states"]

                for flight in states:
                    latitude = flight[6]
                    longitude = flight[5]

                    print("Latitude:", latitude, "Longitude:", longitude)

    finally:
        consumer.close()

if __name__ == "__main__":
    main()