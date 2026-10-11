"""
incident_demand_producer.py
=============================
Replays incident_demand.csv onto Kafka one scenario-second at a time.
Mirrors traffic_speed_producer.py exactly, just pointed at a different
CSV and topic -- this symmetry is deliberate so your teammates only need
to understand the pattern once.
"""

import csv
import json
import time

from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_INCIDENT

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def load_rows(filename="incident_demand.csv"):
    """Read the CSV, converting each field to the right Python type."""
    rows = []
    with open(filename, newline="") as f:
        for row in csv.DictReader(f):
            row["scenario_second"] = int(row["scenario_second"])
            row["active_incidents"] = int(row["active_incidents"])
            row["severity"] = float(row["severity"])
            rows.append(row)
    return rows


def main():
    rows = load_rows()
    max_second = max(r["scenario_second"] for r in rows)
    print(f"Incident producer: continuous replay of {max_second + 1}-second scenario.")
    print("Press Ctrl+C to stop.")

    cycle = 1
    try:
        while True:
            print(f"\n--- Incident scenario cycle {cycle} ---")
            for second in range(max_second + 1):
                batch = [r for r in rows if r["scenario_second"] == second]
                for row in batch:
                    producer.send(TOPIC_INCIDENT, value=row)
                producer.flush()
                print(f"  [cycle={cycle} t={second:>3}s] sent {len(batch)} incident readings")
                time.sleep(1)
            cycle += 1
    except KeyboardInterrupt:
        print("\nIncident producer stopped by user.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
