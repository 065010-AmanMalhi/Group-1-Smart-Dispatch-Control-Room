"""
config.py
=========
This is the ONE file that controls the entire pipeline's behaviour.

Why this matters for your demo: every producer, the consumer's pricing/ETA
formula, and the scenario generator all import their numbers from here.
To show "one change cascades across every dashboard", you only need to
change a value in THIS file (or in the SCENARIO_EVENTS dict inside
generate_data.py) and re-run the pipeline — you never need to touch
multiple files to change the story.
"""

# ---------------------------------------------------------------------------
# 1. CITY ZONES
# ---------------------------------------------------------------------------
# Five Delhi zones. Every stream (traffic, incidents, weather/road events)
# and every Grafana panel is keyed by one of these names.
ZONES = [
    "Connaught Place",
    "AIIMS Corridor",
    "Ring Road South",
    "Dwarka Expressway",
    "Nehru Place",
]

# Free-flow (normal, no-traffic) speed per zone, in km/h.
# The formula measures delay as "how far below this speed are we right now".
BASELINE_SPEED_KMPH = {
    "Connaught Place": 28,
    "AIIMS Corridor": 32,
    "Ring Road South": 45,
    "Dwarka Expressway": 55,
    "Nehru Place": 30,
}

# Baseline ambulance ETA (in minutes) to any incident in that zone, under
# free-flow conditions. The LIVE ETA shown on the dashboard is simply:
#     live_eta = BASELINE_ETA_MIN[zone] * final_multiplier
BASELINE_ETA_MIN = {
    "Connaught Place": 6,
    "AIIMS Corridor": 4,
    "Ring Road South": 9,
    "Dwarka Expressway": 12,
    "Nehru Place": 7,
}

# ---------------------------------------------------------------------------
# 2. KAFKA SETTINGS
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"   # change only if your broker is elsewhere

# Topic names are suffixed "-sc" (smart city) so they never collide with
# topics you used in earlier lectures on the same Kafka instance.
TOPIC_TRAFFIC = "traffic-speed-sc"
TOPIC_INCIDENT = "incident-demand-sc"
TOPIC_CONTEXT = "zone-context-sc"
TOPIC_AMBULANCE = "ambulance-position-sc"

# ---------------------------------------------------------------------------
# 3. MYSQL SETTINGS
# ---------------------------------------------------------------------------
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"              # match your sda-mysql container's root password
MYSQL_DATABASE = "sda_smart_dispatch"  # isolated DB — won't touch any earlier lecture's data

# ---------------------------------------------------------------------------
# 4. THE CASCADING FORMULA — THE MAIN "TWEAK ME" SECTION FOR YOUR DEMO
# ---------------------------------------------------------------------------
# final_multiplier interpretation:
#   1.0 = normal conditions, no extra delay
#   3.0 = worst-case gridlock (hard ceiling, see MAX_MULTIPLIER)
#
# live_eta_minutes = BASELINE_ETA_MIN[zone] * final_multiplier
#
# Changing any ONE weight below and re-running the consumer changes every
# zone's multiplier, which changes every panel on the Grafana dashboard —
# this is the "single value change cascades everywhere" behaviour asked for.

WEIGHT_SPEED_DROP = 0.40   # how much a traffic slowdown drives up delay
WEIGHT_INCIDENT = 0.35     # how much active-incident severity drives up delay
WEIGHT_WEATHER = 0.15      # how much rain/fog drives up delay
WEIGHT_ROAD_EVENT = 0.30   # how much a closure / VIP movement / construction drives up delay

MIN_MULTIPLIER = 1.0    # floor — conditions can never be "better than normal"
MAX_MULTIPLIER = 3.0    # ceiling — caps how bad the ETA can look, even in chaos
MAX_STEP_CHANGE = 0.25  # smoothing: the multiplier can move at most this much per
                         # decision cycle, so one noisy reading can't cause a wild jump

AMBULANCE_UPDATE_SECONDS = 5  # live ambulance GPS update interval
AMBULANCE_SIM_TIME_SCALE = 10.0  # 1 real second simulates 10 seconds of travel

WINDOW_SECONDS = 10  # the consumer re-evaluates every zone once per this many seconds

# ---------------------------------------------------------------------------
# 4b. ALERT / TRIGGER SYSTEM
# ---------------------------------------------------------------------------
# Every time a zone's final_multiplier crosses into a new severity tier, the
# consumer writes a row to the `alerts_log` table (this is what powers the
# "Active Alerts" panel in Grafana) and prints a console banner. Crossing
# UP into WARNING or CRITICAL also optionally sends an email (see below).
#
# Tuned against this scenario's actual peak values (Ring Road ~1.6x,
# Connaught ~1.5x, AIIMS ~1.3x) so the demo reliably produces one CRITICAL
# zone, two WARNING zones, and two zones that stay NORMAL throughout.
ALERT_WARNING_MULTIPLIER = 1.3
ALERT_CRITICAL_MULTIPLIER = 1.55

# --- Optional email notifications -------------------------------------------
# OFF by default so the pipeline runs with zero extra setup. Flip this on and
# fill in the SMTP_* values below only if you want real emails during the
# demo -- see README.md "Enabling email alerts" for the exact Gmail steps.
EMAIL_ALERTS_ENABLED = False

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465
SMTP_SENDER_EMAIL = "your_project_email@gmail.com"
SMTP_SENDER_APP_PASSWORD = "xxxx xxxx xxxx xxxx"   # Gmail "App Password", NOT your normal password
ALERT_RECIPIENT_EMAIL = "your_team_email@gmail.com"

# ---------------------------------------------------------------------------
# 5. SCENARIO TIMING (used by generate_data.py)
# ---------------------------------------------------------------------------
# The whole scenario plays out in three scripted phases so your live demo
# has a reliable rhythm: calm -> crisis -> recovery, over about 75 seconds.
PHASE_NORMAL_END = 20     # seconds 0-20   : calm baseline, all zones normal
PHASE_INCIDENT_END = 50   # seconds 20-50  : incidents/weather/closures hit specific zones
PHASE_RECOVERY_END = 75   # seconds 50-75  : conditions ease back toward normal

TOTAL_SCENARIO_SECONDS = PHASE_RECOVERY_END
