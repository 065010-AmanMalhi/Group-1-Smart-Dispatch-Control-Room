"""
traffic_speed_producer.py
==========================
Reads traffic_speed.csv (created by generate_data.py) and replays it onto
Kafka one scenario-second at a time, so it behaves like a live sensor feed.

Run order matters: start dispatch_consumer.py FIRST (it creates the MySQL
schema and starts listening), then start this + the other two producers.
"""

import csv
import json
import time

from kafka import KafkaProducer  # pip install kafka-python

from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_TRAFFIC

# Create the Kafka producer. value_serializer turns our Python dict into
# JSON bytes automatically, since Kafka only transports raw bytes.
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def load_rows(filename="traffic_speed.csv"):
    """Read the CSV into a list of dicts, converting numeric fields from text."""
    rows = []
    with open(filename, newline="") as f:
        for row in csv.DictReader(f):
            row["scenario_second"] = int(row["scenario_second"])
            row["speed_kmph"] = float(row["speed_kmph"])
            rows.append(row)
    return rows


def main():
    rows = load_rows()

    # Replay the finite synthetic scenario continuously. Each completed 75-second
    # scenario loops back to t=0, so the dashboard keeps receiving fresh data
    # until you stop this terminal with Ctrl+C.
    max_second = max(r["scenario_second"] for r in rows)
    print(f"Traffic producer: continuous replay of {max_second + 1}-second scenario.")
    print("Press Ctrl+C to stop.")

    cycle = 1
    try:
        while True:
            print(f"\n--- Traffic scenario cycle {cycle} ---")
            for second in range(max_second + 1):
                batch = [r for r in rows if r["scenario_second"] == second]
                for row in batch:
                    producer.send(TOPIC_TRAFFIC, value=row)
                producer.flush()
                print(f"  [cycle={cycle} t={second:>3}s] sent {len(batch)} traffic readings")
                time.sleep(1)
            cycle += 1
    except KeyboardInterrupt:
        print("\nTraffic producer stopped by user.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
