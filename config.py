"""
Project configuration for Smart Dispatch Control Room.

Runtime connection settings are loaded from environment variables or a local
.env file. Keep .env out of version control and never commit real credentials.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Five Delhi simulation zones.
ZONES = [
    "Connaught Place",
    "AIIMS Corridor",
    "Ring Road South",
    "Dwarka Expressway",
    "Nehru Place",
]

BASELINE_SPEED_KMPH = {
    "Connaught Place": 28,
    "AIIMS Corridor": 32,
    "Ring Road South": 45,
    "Dwarka Expressway": 55,
    "Nehru Place": 30,
}

BASELINE_ETA_MIN = {
    "Connaught Place": 6,
    "AIIMS Corridor": 4,
    "Ring Road South": 9,
    "Dwarka Expressway": 12,
    "Nehru Place": 7,
}

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TRAFFIC = "traffic-speed-sc"
TOPIC_INCIDENT = "incident-demand-sc"
TOPIC_CONTEXT = "zone-context-sc"
TOPIC_AMBULANCE = "ambulance-position-sc"

# MySQL: provide MYSQL_PASSWORD in the local .env file.
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "sda_smart_dispatch")

# Dispatch scoring weights
WEIGHT_SPEED_DROP = 0.40
WEIGHT_INCIDENT = 0.35
WEIGHT_WEATHER = 0.15
WEIGHT_ROAD_EVENT = 0.30

MIN_MULTIPLIER = 1.0
MAX_MULTIPLIER = 3.0
MAX_STEP_CHANGE = 0.25
AMBULANCE_UPDATE_SECONDS = 5
AMBULANCE_SIM_TIME_SCALE = 10.0
WINDOW_SECONDS = 10

# Alert thresholds
ALERT_WARNING_MULTIPLIER = 1.3
ALERT_CRITICAL_MULTIPLIER = 1.55

# Optional email notifications (disabled unless explicitly configured).
EMAIL_ALERTS_ENABLED = False
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465
SMTP_SENDER_EMAIL = os.getenv("SMTP_SENDER_EMAIL", "your_project_email@gmail.com")
SMTP_SENDER_APP_PASSWORD = os.getenv("SMTP_SENDER_APP_PASSWORD", "")
ALERT_RECIPIENT_EMAIL = os.getenv("ALERT_RECIPIENT_EMAIL", "your_team_email@gmail.com")

# Synthetic scenario phases
PHASE_NORMAL_END = 20
PHASE_INCIDENT_END = 50
PHASE_RECOVERY_END = 75
TOTAL_SCENARIO_SECONDS = PHASE_RECOVERY_END
