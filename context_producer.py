"""
context_producer.py
=====================
Replays zone_context.csv (rain intensity + road-event severity) onto Kafka
one scenario-second at a time. Same pattern as the other two producers.
"""

import csv
import json
import time

from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_CONTEXT

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def load_rows(filename="zone_context.csv"):
    """Read the CSV, converting each field to the right Python type."""
    rows = []
    with open(filename, newline="") as f:
        for row in csv.DictReader(f):
            row["scenario_second"] = int(row["scenario_second"])
            row["rain_intensity"] = float(row["rain_intensity"])
            row["road_event_severity"] = float(row["road_event_severity"])
            rows.append(row)
    return rows


def main():
    rows = load_rows()
    max_second = max(r["scenario_second"] for r in rows)
    print(f"Context producer: continuous replay of {max_second + 1}-second scenario.")
    print("Press Ctrl+C to stop.")

    cycle = 1
    try:
        while True:
            print(f"\n--- Context scenario cycle {cycle} ---")
            for second in range(max_second + 1):
                batch = [r for r in rows if r["scenario_second"] == second]
                for row in batch:
                    producer.send(TOPIC_CONTEXT, value=row)
                producer.flush()
                print(f"  [cycle={cycle} t={second:>3}s] sent {len(batch)} context readings")
                time.sleep(1)
            cycle += 1
    except KeyboardInterrupt:
        print("\nContext producer stopped by user.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
