"""
generate_data.py
=================
Generates three synthetic CSV files — one per Kafka stream — that the
producers will later replay second-by-second:

    traffic_speed.csv    -> current speed per zone, every second
    incident_demand.csv  -> active incidents + severity per zone, every second
    zone_context.csv     -> rain intensity + road-event severity per zone, every second

The scenario is SCRIPTED (so your live demo has a guaranteed, repeatable
story) but every row also gets small RANDOM JITTER added, so the lines on
Grafana look like real sensor data instead of a perfectly flat robot line.

Run this once before every demo run:
    python generate_data.py
"""

import csv
import random

# Pull every shared number from config.py so this file never hardcodes
# anything that should be tweakable in one place.
from config import (
    ZONES,
    BASELINE_SPEED_KMPH,
    PHASE_NORMAL_END,
    PHASE_INCIDENT_END,
    PHASE_RECOVERY_END,
    TOTAL_SCENARIO_SECONDS,
)

# Fixed seed so re-running without changing SCENARIO_EVENTS gives you the
# exact same numbers each time -- predictable for rehearsal.
random.seed(42)

# ---------------------------------------------------------------------------
# >>> TWEAK ME FOR YOUR DEMO <<<
# ---------------------------------------------------------------------------
# This dict is the SECOND place (besides config.py's weights) where you can
# change ONE value and get a completely different, cascading outcome once
# you regenerate data and re-run the pipeline.
#
# Each entry describes what happens to that zone DURING the incident phase
# (seconds PHASE_NORMAL_END -> PHASE_INCIDENT_END):
#   speed_factor        -> current speed = baseline_speed * speed_factor
#                          (lower = slower = worse traffic)
#   incident_severity    -> 0.0 (none) to 1.0 (critical)
#   rain_intensity        -> 0.0 (clear) to 1.0 (heavy rain)
#   road_event_severity   -> 0.0 (none) to 1.0 (major closure / VIP movement)
#
# Zones NOT listed here stay calm throughout -> they act as a "control group"
# on the dashboard, which makes the affected zones' spikes obvious by contrast.
SCENARIO_EVENTS = {
    "Ring Road South": {
        "speed_factor": 0.35,       # major accident -> traffic crawls
        "incident_severity": 0.90,
        "rain_intensity": 0.0,
        "road_event_severity": 0.10,
    },
    "AIIMS Corridor": {
        "speed_factor": 0.70,       # heavy rain -> moderate slowdown
        "incident_severity": 0.20,
        "rain_intensity": 0.80,
        "road_event_severity": 0.0,
    },
    "Connaught Place": {
        "speed_factor": 0.50,       # VIP movement -> road partially closed
        "incident_severity": 0.10,
        "rain_intensity": 0.0,
        "road_event_severity": 0.85,
    },
}


def jitter(value, spread=0.05):
    """Add small random noise (+/- `spread` fraction) so data isn't perfectly flat."""
    return value * (1 + random.uniform(-spread, spread))


def get_phase(second):
    """Return which scripted phase a given scenario-second falls into."""
    if second < PHASE_NORMAL_END:
        return "normal"
    elif second < PHASE_INCIDENT_END:
        return "incident"
    else:
        return "recovery"


def recovery_progress(second):
    """
    Returns 0.0 right when recovery starts and 1.0 once fully recovered,
    so conditions ease back gradually instead of snapping to normal.
    """
    span = PHASE_RECOVERY_END - PHASE_INCIDENT_END
    elapsed = second - PHASE_INCIDENT_END
    return min(max(elapsed / span, 0.0), 1.0)


def generate_traffic_rows():
    """Build every row for traffic_speed.csv across the whole scenario."""
    rows = []
    for second in range(TOTAL_SCENARIO_SECONDS):
        phase = get_phase(second)
        for zone in ZONES:
            baseline = BASELINE_SPEED_KMPH[zone]
            event = SCENARIO_EVENTS.get(zone)

            if phase == "normal" or event is None:
                # No scripted event for this zone -> stays near baseline.
                speed = jitter(baseline)
            elif phase == "incident":
                speed = jitter(baseline * event["speed_factor"])
            else:  # recovery -> blend back from "incident speed" to baseline
                progress = recovery_progress(second)
                incident_speed = baseline * event["speed_factor"]
                speed = jitter(incident_speed + (baseline - incident_speed) * progress)

            rows.append({"scenario_second": second, "zone": zone, "speed_kmph": round(speed, 1)})
    return rows


def generate_incident_rows():
    """Build every row for incident_demand.csv across the whole scenario."""
    rows = []
    for second in range(TOTAL_SCENARIO_SECONDS):
        phase = get_phase(second)
        for zone in ZONES:
            event = SCENARIO_EVENTS.get(zone)

            if phase == "normal" or event is None:
                severity = jitter(0.05, spread=0.5)   # near-zero baseline noise
                active_incidents = 0 if random.random() > 0.1 else 1
            elif phase == "incident":
                severity = jitter(event["incident_severity"])
                active_incidents = 1 if event["incident_severity"] > 0 else 0
            else:  # recovery -> severity fades out
                progress = recovery_progress(second)
                severity = jitter(event["incident_severity"] * (1 - progress))
                active_incidents = 1 if severity > 0.15 else 0

            rows.append({
                "scenario_second": second,
                "zone": zone,
                "active_incidents": active_incidents,
                "severity": round(max(severity, 0), 2),
            })
    return rows


def generate_context_rows():
    """Build every row for zone_context.csv (weather + road events) across the scenario."""
    rows = []
    for second in range(TOTAL_SCENARIO_SECONDS):
        phase = get_phase(second)
        for zone in ZONES:
            event = SCENARIO_EVENTS.get(zone)

            if phase == "normal" or event is None:
                rain = jitter(0.02, spread=0.5)
                road_event = jitter(0.02, spread=0.5)
            elif phase == "incident":
                rain = jitter(event["rain_intensity"]) if event["rain_intensity"] > 0 else jitter(0.02, spread=0.5)
                road_event = jitter(event["road_event_severity"]) if event["road_event_severity"] > 0 else jitter(0.02, spread=0.5)
            else:  # recovery -> fade both back down
                progress = recovery_progress(second)
                rain = jitter(event["rain_intensity"] * (1 - progress))
                road_event = jitter(event["road_event_severity"] * (1 - progress))

            rows.append({
                "scenario_second": second,
                "zone": zone,
                "rain_intensity": round(max(rain, 0), 2),
                "road_event_severity": round(max(road_event, 0), 2),
            })
    return rows


def write_csv(filename, rows, fieldnames):
    """Small helper: write a list of dict rows to a CSV file with a header."""
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows -> {filename}")


if __name__ == "__main__":
    write_csv("traffic_speed.csv", generate_traffic_rows(), ["scenario_second", "zone", "speed_kmph"])
    write_csv("incident_demand.csv", generate_incident_rows(), ["scenario_second", "zone", "active_incidents", "severity"])
    write_csv("zone_context.csv", generate_context_rows(), ["scenario_second", "zone", "rain_intensity", "road_event_severity"])
    print(f"\nScenario covers {TOTAL_SCENARIO_SECONDS} seconds across {len(ZONES)} zones.")
    print("Scripted events hit:", ", ".join(SCENARIO_EVENTS.keys()))
