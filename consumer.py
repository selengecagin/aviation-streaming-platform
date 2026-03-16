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
                flight = json.loads(value)

                icao24 = flight[0]
                callsign = flight[1]
                origin_country = flight[2]
                timestamp = flight[3]
                longitude = flight[5]
                latitude = flight[6]
                altitude = flight[7]
                on_ground = flight[8]
                velocity = flight[9]
                heading = flight[10]

                print(
                    f"ICAO24: {icao24}, Callsign: {callsign}, Country: {origin_country}, "
                    f"Time: {timestamp}, Lat: {latitude}, Lon: {longitude}, Alt: {altitude}, "
                    f"On Ground: {on_ground}, Velocity: {velocity}, Heading: {heading}"
                )

    finally:
        consumer.close()

if __name__ == "__main__":
    main()