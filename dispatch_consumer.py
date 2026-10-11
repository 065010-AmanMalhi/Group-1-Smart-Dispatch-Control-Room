"""
dispatch_consumer.py
======================
The brain of the pipeline. This script:

  1. Creates the MySQL database/table on startup (so you never need to run
     manual SQL first).
  2. Subscribes to all three Kafka topics (traffic, incidents, context) and
     keeps the LATEST known reading for every zone in memory.
  3. Every WINDOW_SECONDS, recomputes the "final_multiplier" and "live ETA"
     for every zone using the formula in config.py, smooths it so it can't
     jump too fast, and writes a full audit row to MySQL.

START THIS FIRST, before any of the three producers -- it needs to create
the schema and be listening before data starts arriving.
"""

import json
import smtplib
import ssl
import time
from email.mime.text import MIMEText

import mysql.connector
from kafka import KafkaConsumer

from config import (
    ZONES,
    BASELINE_SPEED_KMPH,
    BASELINE_ETA_MIN,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_TRAFFIC,
    TOPIC_INCIDENT,
    TOPIC_CONTEXT,
    TOPIC_AMBULANCE,
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
    WEIGHT_SPEED_DROP,
    WEIGHT_INCIDENT,
    WEIGHT_WEATHER,
    WEIGHT_ROAD_EVENT,
    MIN_MULTIPLIER,
    MAX_MULTIPLIER,
    MAX_STEP_CHANGE,
    WINDOW_SECONDS,
    ALERT_WARNING_MULTIPLIER,
    ALERT_CRITICAL_MULTIPLIER,
    EMAIL_ALERTS_ENABLED,
    SMTP_SERVER,
    SMTP_PORT,
    SMTP_SENDER_EMAIL,
    SMTP_SENDER_APP_PASSWORD,
    ALERT_RECIPIENT_EMAIL,
)


# ---------------------------------------------------------------------------
# STEP 1: MySQL setup
# ---------------------------------------------------------------------------
def setup_database():
    """
    Connects WITHOUT selecting a database (so we can CREATE it if missing),
    then creates the dispatch_decisions table if it doesn't already exist.
    Returns an open connection with MYSQL_DATABASE already selected.
    """
    # First connection: no database selected yet.
    conn = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE}")
    cursor.execute(f"USE {MYSQL_DATABASE}")

    # The audit table: every decision we ever make gets a row here, with the
    # raw inputs AND the final result, so Grafana's "factor breakdown" panel
    # can explain WHY the multiplier is what it is.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dispatch_decisions (
            decision_id INT AUTO_INCREMENT PRIMARY KEY,
            decision_time DATETIME DEFAULT CURRENT_TIMESTAMP,
            zone VARCHAR(50),
            current_speed_kmph FLOAT,
            baseline_speed_kmph FLOAT,
            speed_drop_pct FLOAT,
            active_incidents INT,
            incident_severity FLOAT,
            rain_intensity FLOAT,
            road_event_severity FLOAT,
            raw_multiplier FLOAT,
            final_multiplier FLOAT,
            baseline_eta_min FLOAT,
            live_eta_min FLOAT,
            reason VARCHAR(255)
        )
    """)

    # The alerts table is separate from dispatch_decisions on purpose: it
    # only gets a new row when a zone CHANGES severity tier (not every
    # 10-second cycle), so it reads as a clean trigger log rather than a
    # noisy duplicate of the audit table. This is what the "Active Alerts"
    # Grafana panel displays.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alerts_log (
            alert_id INT AUTO_INCREMENT PRIMARY KEY,
            triggered_time DATETIME DEFAULT CURRENT_TIMESTAMP,
            zone VARCHAR(50),
            alert_level VARCHAR(20),
            final_multiplier FLOAT,
            live_eta_min FLOAT,
            message VARCHAR(255)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hospitals (
            hospital_id INT AUTO_INCREMENT PRIMARY KEY,
            hospital_name VARCHAR(120) UNIQUE,
            address VARCHAR(255),
            latitude DOUBLE,
            longitude DOUBLE,
            zone VARCHAR(50)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ambulance_routes (
            route_id INT AUTO_INCREMENT PRIMARY KEY,
            ambulance_id VARCHAR(20) NOT NULL,
            route_seq INT NOT NULL,
            latitude DOUBLE NOT NULL,
            longitude DOUBLE NOT NULL,
            cumulative_distance_km DOUBLE NOT NULL,
            UNIQUE KEY uq_route (ambulance_id, route_seq)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ambulance_positions (
            position_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            event_time DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
            ambulance_id VARCHAR(20) NOT NULL,
            hospital VARCHAR(120),
            destination VARCHAR(120),
            zone VARCHAR(50),
            latitude DOUBLE NOT NULL,
            longitude DOUBLE NOT NULL,
            speed_kmph DOUBLE,
            route_progress_pct DOUBLE,
            remaining_distance_km DOUBLE,
            eta_min DOUBLE,
            final_multiplier DOUBLE,
            status VARCHAR(30)
        )
    """)

    # Real Delhi facilities from Delhi Government listings; coordinates are
    # approximate simulation points used only to place the map markers.
    hospitals = [
        ("AIIMS Hospital", "Ansari Nagar, New Delhi-110029", 28.5672, 77.2100, "AIIMS Corridor"),
        ("Max Super Speciality Hospital, Patparganj", "108A, IP Extension, Patparganj, Delhi-110092", 28.6209, 77.2940, "Ring Road South"),
        ("Ram Manohar Lohia Hospital", "Baba Kharak Singh Marg, Delhi", 28.6250, 77.1994, "Connaught Place"),
        ("Fortis Hospital Shalimar Bagh", "A Block, Shaheed Udham Singh Marg, Shalimar Bagh, Delhi", 28.7055, 77.1593, "Ring Road South"),
        ("Aakash Healthcare, Super Specialty Hospital", "Hospital Plot, Road No. 201, Sector-3, Dwarka, Delhi-110075", 28.5789, 77.0717, "Dwarka Expressway"),
    ]
    cursor.executemany("INSERT INTO hospitals (hospital_name,address,latitude,longitude,zone) VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE address=VALUES(address), latitude=VALUES(latitude), longitude=VALUES(longitude), zone=VALUES(zone)", hospitals)

    conn.commit()
    cursor.close()
    print(f"MySQL ready: database '{MYSQL_DATABASE}', tables 'dispatch_decisions' + 'alerts_log'.")
    return conn


# ---------------------------------------------------------------------------
# STEP 2: In-memory "latest known state" per zone
# ---------------------------------------------------------------------------
# These dicts hold the most recent reading we've seen for each zone from
# each of the three streams. They start at sensible defaults (normal
# conditions) so the formula works even before the first message arrives.
latest_speed = {zone: BASELINE_SPEED_KMPH[zone] for zone in ZONES}
latest_incidents = {zone: {"active_incidents": 0, "severity": 0.0} for zone in ZONES}
latest_context = {zone: {"rain_intensity": 0.0, "road_event_severity": 0.0} for zone in ZONES}

# Remembers each zone's last published multiplier, so we can rate-limit
# (MAX_STEP_CHANGE) how fast it's allowed to move -- this is what makes the
# dashboard ease into a crisis instead of jumping instantly.
previous_multiplier = {zone: MIN_MULTIPLIER for zone in ZONES}

# Remembers each zone's last ALERT TIER (not the raw multiplier), so we only
# write a new alerts_log row / send an email when a zone actually crosses
# into a different tier -- not on every single decision cycle.
previous_level = {zone: "NORMAL" for zone in ZONES}


# ---------------------------------------------------------------------------
# STEP 3b: Alert tier classification + the trigger itself
# ---------------------------------------------------------------------------
def classify_level(multiplier):
    """Maps a final_multiplier value to a severity tier using config.py's thresholds."""
    if multiplier >= ALERT_CRITICAL_MULTIPLIER:
        return "CRITICAL"
    elif multiplier >= ALERT_WARNING_MULTIPLIER:
        return "WARNING"
    else:
        return "NORMAL"


def send_alert_email(zone, level, decision):
    """
    Sends a real email via Gmail's SMTP server. Only called when
    EMAIL_ALERTS_ENABLED is True in config.py. Wrapped in try/except so a
    bad password or no internet connection never crashes the pipeline --
    it just prints the failure and keeps going.
    """
    subject = f"[{level}] Ambulance dispatch delay in {zone}"
    body = (
        f"Zone: {zone}\n"
        f"Severity: {level}\n"
        f"Final multiplier: {decision['final_multiplier']}x\n"
        f"Live ETA: {decision['live_eta_min']} minutes "
        f"(baseline {decision['baseline_eta_min']} minutes)\n"
        f"Reason: {decision['reason']}\n"
    )
    message = MIMEText(body)
    message["Subject"] = subject
    message["From"] = SMTP_SENDER_EMAIL
    message["To"] = ALERT_RECIPIENT_EMAIL

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
            server.login(SMTP_SENDER_EMAIL, SMTP_SENDER_APP_PASSWORD)
            server.sendmail(SMTP_SENDER_EMAIL, ALERT_RECIPIENT_EMAIL, message.as_string())
        print(f"    -> email alert sent to {ALERT_RECIPIENT_EMAIL}")
    except Exception as e:
        print(f"    -> email alert FAILED (pipeline continues): {e}")


def log_alert(conn, zone, level, decision):
    """Insert one row into alerts_log and print a console banner."""
    cursor = conn.cursor()
    message = f"{zone} moved to {level} (ETA {decision['live_eta_min']} min, {decision['reason']})"
    cursor.execute(
        """INSERT INTO alerts_log (zone, alert_level, final_multiplier, live_eta_min, message)
           VALUES (%s, %s, %s, %s, %s)""",
        (zone, level, decision["final_multiplier"], decision["live_eta_min"], message),
    )
    conn.commit()
    cursor.close()
    print(f"  >>> ALERT [{level}] {message}")


# ---------------------------------------------------------------------------
# STEP 3: The cascading formula
# ---------------------------------------------------------------------------
def compute_decision(zone):
    """
    Pulls this zone's latest state from all three streams, runs the
    weighted formula from config.py, applies the min/max cap and the
    rate-limit, and returns a dict ready to insert into MySQL.
    """
    baseline_speed = BASELINE_SPEED_KMPH[zone]
    current_speed = latest_speed[zone]
    speed_drop_pct = max(baseline_speed - current_speed, 0) / baseline_speed

    incident = latest_incidents[zone]
    context = latest_context[zone]

    # Each adjustment term is "how bad is this factor" x "how much we care about it".
    speed_adj = speed_drop_pct * WEIGHT_SPEED_DROP
    incident_adj = incident["severity"] * WEIGHT_INCIDENT
    weather_adj = context["rain_intensity"] * WEIGHT_WEATHER
    road_event_adj = context["road_event_severity"] * WEIGHT_ROAD_EVENT

    raw_multiplier = 1 + speed_adj + incident_adj + weather_adj + road_event_adj

    # Cap it to a believable range (1.0x to 3.0x).
    target_multiplier = min(max(raw_multiplier, MIN_MULTIPLIER), MAX_MULTIPLIER)

    # Rate-limit: move at most MAX_STEP_CHANGE per decision, toward the target.
    prev = previous_multiplier[zone]
    if target_multiplier > prev:
        final_multiplier = min(prev + MAX_STEP_CHANGE, target_multiplier)
    else:
        final_multiplier = max(prev - MAX_STEP_CHANGE, target_multiplier)
    previous_multiplier[zone] = final_multiplier

    live_eta = BASELINE_ETA_MIN[zone] * final_multiplier

    # Build a human-readable "why" string using whichever factor contributed most --
    # this is what makes the system explainable rather than a black box.
    contributions = {
        "heavy traffic": speed_adj,
        "active incident": incident_adj,
        "weather": weather_adj,
        "road closure/event": road_event_adj,
    }
    top_factor = max(contributions, key=contributions.get)
    if contributions[top_factor] < 0.05:
        reason = "normal conditions"
    else:
        reason = f"delay mainly due to {top_factor}"

    return {
        "zone": zone,
        "current_speed_kmph": round(current_speed, 1),
        "baseline_speed_kmph": baseline_speed,
        "speed_drop_pct": round(speed_drop_pct, 2),
        "active_incidents": incident["active_incidents"],
        "incident_severity": round(incident["severity"], 2),
        "rain_intensity": round(context["rain_intensity"], 2),
        "road_event_severity": round(context["road_event_severity"], 2),
        "raw_multiplier": round(raw_multiplier, 2),
        "final_multiplier": round(final_multiplier, 2),
        "baseline_eta_min": BASELINE_ETA_MIN[zone],
        "live_eta_min": round(live_eta, 1),
        "reason": reason,
    }


def save_decision(conn, decision):
    """Insert one decision row into MySQL."""
    cursor = conn.cursor()
    columns = ", ".join(decision.keys())
    placeholders = ", ".join(["%s"] * len(decision))
    cursor.execute(
        f"INSERT INTO dispatch_decisions ({columns}) VALUES ({placeholders})",
        list(decision.values()),
    )
    conn.commit()
    cursor.close()


# ---------------------------------------------------------------------------
# STEP 4: Kafka consumption loop
# ---------------------------------------------------------------------------
def main():
    conn = setup_database()

    # One consumer subscribed to all three topics at once -- messages from
    # different topics arrive interleaved, which is fine since we just
    # update whichever dict matches the topic each message came from.
    consumer = KafkaConsumer(
        TOPIC_TRAFFIC,
        TOPIC_INCIDENT,
        TOPIC_CONTEXT,
        TOPIC_AMBULANCE,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",   # only read NEW messages from when we start
        consumer_timeout_ms=500,      # poll() returns every 0.5s even with no messages,
    )                                  # so our WINDOW_SECONDS timer keeps ticking

    print(f"Consumer listening on: {TOPIC_TRAFFIC}, {TOPIC_INCIDENT}, {TOPIC_CONTEXT}, {TOPIC_AMBULANCE}")
    print(f"Writing a decision for every zone every {WINDOW_SECONDS} seconds...\n")

    last_decision_time = time.time()

    while True:
        # Drain whatever messages have arrived in the last 0.5s and update state.
        for message in consumer:
            topic = message.topic
            value = message.value
            zone = value["zone"]

            if topic == TOPIC_TRAFFIC:
                latest_speed[zone] = value["speed_kmph"]
            elif topic == TOPIC_INCIDENT:
                latest_incidents[zone] = {
                    "active_incidents": value["active_incidents"],
                    "severity": value["severity"],
                }
            elif topic == TOPIC_CONTEXT:
                latest_context[zone] = {
                    "rain_intensity": value["rain_intensity"],
                    "road_event_severity": value["road_event_severity"],
                }
            elif topic == TOPIC_AMBULANCE:
                cursor = conn.cursor()
                cursor.execute("""INSERT INTO ambulance_positions
                    (ambulance_id,hospital,destination,zone,latitude,longitude,speed_kmph,route_progress_pct,remaining_distance_km,eta_min,final_multiplier,status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (value["ambulance_id"], value["hospital"], value["destination"], value["zone"],
                     value["latitude"], value["longitude"], value["speed_kmph"], value["route_progress_pct"],
                     value["remaining_distance_km"], value["eta_min"], value["final_multiplier"], value["status"]))
                conn.commit()
                cursor.close()

        # Every WINDOW_SECONDS, recompute + store a decision for every zone.
        if time.time() - last_decision_time >= WINDOW_SECONDS:
            for zone in ZONES:
                decision = compute_decision(zone)
                save_decision(conn, decision)
                print(
                    f"  [{zone}] mult={decision['final_multiplier']}x  "
                    f"ETA={decision['live_eta_min']}min  ({decision['reason']})"
                )

                # Check whether this zone just crossed into a different
                # severity tier (in either direction) since last time.
                level = classify_level(decision["final_multiplier"])
                if level != previous_level[zone]:
                    log_alert(conn, zone, level, decision)
                    # Only email on the way UP into trouble, never on recovery.
                    if EMAIL_ALERTS_ENABLED and level in ("WARNING", "CRITICAL"):
                        send_alert_email(zone, level, decision)
                    previous_level[zone] = level

            last_decision_time = time.time()


if __name__ == "__main__":
    main()
