# Smart Dispatch Control Room — Setup & Demo Guide

Emergency vehicle (ambulance/fire truck) dispatch delay engine across 5 Delhi
zones. Kafka → Python consumer with an explainable formula → MySQL → Grafana,
all live-updating.

## 0. Prerequisites (one-time)

Create and activate a virtual environment in the project folder, then
install dependencies into it:

```bash
# from inside the smart_dispatch/ folder
python -m venv venv

# activate it:
venv\Scripts\activate        # Windows (PowerShell/cmd)
source venv/bin/activate     # macOS/Linux

# then install dependencies (inside the activated venv)
pip install -r requirements.txt
```

You'll know it's active when your terminal prompt shows `(venv)` at the
start of the line. **Every terminal you open for this project (all 4 in the
run-order table below) needs the venv activated first** — VS Code usually
auto-activates it per terminal if you select the venv's Python interpreter
as your workspace interpreter (bottom-right corner in VS Code, or
`Ctrl+Shift+P` → "Python: Select Interpreter" → pick `./venv/...`).

**Important — Kafka library note:** `requirements.txt` installs
`kafka-python-ng`, not plain `kafka-python`. The original `kafka-python`
package is broken on Python 3.12+ (a `ModuleNotFoundError:
kafka.vendor.six.moves` error) — `kafka-python-ng` is the maintained fork
that fixes this, and it's a drop-in replacement (same `from kafka import
...` lines in all the scripts, no code changes needed). If you already ran
`pip install kafka-python` manually before reading this, undo it first:
```bash
pip uninstall kafka-python -y
pip install kafka-python-ng
```

Your containers (confirmed via `docker ps -a`), all already **Up** — nothing
to start:

| Container | Image | Port |
|---|---|---|
| `sda-mysql-1` | mysql:8 | `3306:3306` (matches `config.py`'s `MYSQL_PORT`) |
| `sda-kafka-1` | wurstmeister/kafka | `9092:9092` (matches `config.py`'s `KAFKA_BOOTSTRAP_SERVERS`) |
| `sda-zookeeper-1` | wurstmeister/zookeeper | `2181:2181` |
| `sda-grafana` | grafana/grafana | `3002:3000` — **host port is 3002, not Grafana's default 3000.** Open Grafana at **http://localhost:3002** |

If any of these ever stop, restart with:
```bash
docker start sda-mysql-1 sda-kafka-1 sda-zookeeper-1 sda-grafana
```

`config.py` already matches your Kafka and MySQL ports. The one thing you
still need to confirm yourself is `MYSQL_PASSWORD` in `config.py` — set it
to whatever root password your `sda-mysql-1` container actually uses (check
your Lecture 16 setup notes if unsure).

## 1. Generate synthetic data (do this before every demo run)

```bash
python generate_data.py
```

This writes `traffic_speed.csv`, `incident_demand.csv`, and `zone_context.csv`
— a scripted 75-second scenario: 20s calm, 30s incidents hit 3 zones
(Ring Road South = major accident, AIIMS Corridor = heavy rain,
Connaught Place = VIP movement/road closure), then 25s recovery.
Dwarka Expressway and Nehru Place stay calm the whole time as a visible
"control group" contrast on the dashboard.

## 2. Run order (use 4 separate terminals)

| Terminal | Command | Role |
|---|---|---|
| 1 | `python dispatch_consumer.py` | **Start first.** Creates the MySQL schema, listens to all 3 topics |
| 2 | `python traffic_speed_producer.py` | Traffic speed stream |
| 3 | `python incident_demand_producer.py` | Incident stream |
| 4 | `python context_producer.py` | Weather/road-event stream |

Start terminal 1 first and wait for "Consumer listening on...". Then start
terminals 2-4 close together. The three producers continuously replay the
75-second scenario in a loop, so the dashboard keeps updating until you stop
the producer terminals with Ctrl+C.

## 3. Grafana setup (one-time, before the demo)

1. Open Grafana at **http://localhost:3002** (not `:3000` — see the port note
   above).
2. Add a MySQL data source: **Host** `sda-mysql-1:3306` if `sda-grafana` and
   `sda-mysql-1` are on the same Docker network (try this first — container
   name as hostname is the standard way containers reach each other), or
   `host.docker.internal:3306` if that doesn't resolve. **Database**
   `sda_smart_dispatch`, **User/Password** your MySQL credentials. Disable
   TLS for the local lab.
3. **Dashboards → Import → Upload JSON file** → select
   `grafana_dashboard_smart_dispatch.json`.
4. When prompted, map the "MySQL" input to the data source you just created.
5. Open the dashboard, set the time range to **Last 5 minutes**, refresh **5s**
   (already set by default in the JSON, just confirm).

You get 6 panels, all filterable by the `zone` variable at the top:
- **🚨 Active Alerts** (table, colour-coded red/orange/green) — fires the
  moment a zone crosses into WARNING or CRITICAL, this is your trigger system
- **Current ETA Multiplier by Zone** (bar gauge) — the instant snapshot
- **Multiplier Over Time** (all zones) — shows the cascade timing
- **Traffic Speed vs Incident Severity** (filter to ONE zone for a clean read) — cause vs effect
- **Latest Factor Breakdown** (table) — why each zone's multiplier is what it is
- **Dispatch Audit Log** (table) — full history with reason codes

**If you already imported the dashboard before this update:** re-import the
same JSON file — Grafana will detect the matching `uid` and ask if you want
to overwrite the existing dashboard. Say yes, and re-map the MySQL datasource
if it asks again.

## 4. Alert / trigger system

Every time a zone's `final_multiplier` crosses into a different severity
tier, the consumer writes a row to a separate `alerts_log` table and prints
a console banner like:
```
  >>> ALERT [CRITICAL] Ring Road South moved to CRITICAL (ETA 14.4 min, delay mainly due to active incident)
```
This is what the **🚨 Active Alerts** panel shows — a clean trigger log, not
just another copy of the raw data. Thresholds are in `config.py`:
`ALERT_WARNING_MULTIPLIER = 1.3`, `ALERT_CRITICAL_MULTIPLIER = 1.55` — tuned
so this scenario reliably produces one CRITICAL zone (Ring Road South),
two WARNING zones (Connaught Place, AIIMS Corridor), and two zones that stay
NORMAL the whole time.

### Enabling email alerts (optional)

Email is OFF by default (`EMAIL_ALERTS_ENABLED = False` in `config.py`) so
the pipeline needs zero extra setup to run. To make it real for your demo:

1. On the Gmail account you want to send FROM: enable 2-Step Verification,
   then create an **App Password** at
   https://myaccount.google.com/apppasswords (choose "Mail" as the app).
   Gmail gives you a 16-character password — this is NOT your login password.
2. In `config.py`, set:
   ```python
   EMAIL_ALERTS_ENABLED = True
   SMTP_SENDER_EMAIL = "your_project_email@gmail.com"
   SMTP_SENDER_APP_PASSWORD = "the 16-character app password"
   ALERT_RECIPIENT_EMAIL = "whoever should receive it@gmail.com"
   ```
3. Re-run `dispatch_consumer.py`. An email fires the moment any zone first
   crosses into WARNING or CRITICAL (not on every cycle, and not on
   recovery). Test your credentials once BEFORE the live demo — a bad app
   password fails silently into the console log (`email alert FAILED`)
   rather than crashing the pipeline, which is safe but means a typo won't
   be obvious unless you check.

## 6. How to change the story (the "single value" tweak for your demo)

To show the grading panel that the system genuinely reacts to inputs, re-run
with a changed value and regenerate + replay:

- **In `generate_data.py`**: edit any number inside `SCENARIO_EVENTS` (e.g.
  change Ring Road South's `incident_severity` from `0.90` to `0.20`) →
  re-run `python generate_data.py` → re-run the 4 scripts → the whole
  dashboard reacts differently.
- **In `config.py`**: edit a weight (e.g. `WEIGHT_INCIDENT`) → re-run just
  `dispatch_consumer.py` with the same CSVs → see how much more/less the
  SAME incident now affects the multiplier. This is the cleanest "policy
  change" demo, since only the formula changed, not the underlying event.

## 7. Quick MySQL sanity check (optional)

```bash
docker exec sda-mysql-1 mysql -uroot -p -e "
SELECT zone, MAX(final_multiplier) AS peak, MAX(live_eta_min) AS worst_eta
FROM sda_smart_dispatch.dispatch_decisions GROUP BY zone ORDER BY peak DESC;"
```
(it will prompt for the password interactively instead of it being visible
in the command)

## 8. 60-second presentation script

1. **Situation:** "Our dispatch engine tracks 5 Delhi zones live — traffic
   speed, active incidents, and weather/road events."
2. **Trigger:** Point at the 🚨 Active Alerts panel — "The moment a zone's
   delay crosses a threshold, it's logged here automatically — Ring Road
   South just escalated to CRITICAL. This is wired to send an email in
   production, not just light up a dashboard."
3. **Evidence:** Point at the bar gauge — "Watch Ring Road South and
   Connaught Place spike as the accident and VIP closure hit."
4. **Explain:** Open the factor-breakdown table — "The system tells us WHY:
   Ring Road is an incident, Connaught Place is a road closure, AIIMS is rain."
5. **Impact:** "ETA at Ring Road nearly doubles, from 9 to ~14 minutes — that's
   the kind of delay that matters in a cardiac emergency."
6. **Recovery:** "And once conditions clear, both the multiplier and the
   alert ease back to NORMAL automatically — that's intentional smoothing,
   not noise."

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| No rows in MySQL | A producer hasn't started, or consumer started after producers | Always start `dispatch_consumer.py` first |
| Dashboard empty | Wrong database name or time range | Use `sda_smart_dispatch`, Last 5 minutes |
| Multiplier jumps instantly | `MAX_STEP_CHANGE` set too high, or re-running on old data | Lower it in `config.py`, or regenerate data |
| "Table already exists" errors | Re-running without resetting | Fine to ignore — table persists across runs. To reset: `DROP DATABASE sda_smart_dispatch;` in MySQL |
| `ModuleNotFoundError: kafka.vendor.six.moves` | `kafka-python` (not `-ng`) installed on Python 3.12+ | `pip uninstall kafka-python -y && pip install kafka-python-ng` |
| `No such container: mysql-1` etc. | Real container names are `sda-mysql-1`, `sda-kafka-1`, `sda-zookeeper-1` | Use the full names shown above, or `docker ps -a` to re-check |


## 9. LIVE AMBULANCE FLEET (NEW)

The project now includes a fifth Kafka stream for synthetic ambulance GPS.
The five hospital names/addresses are based on Delhi Government hospital
listings. The GPS coordinates, routes, destinations and telemetry are
simulation data for the academic dashboard.

### Start order

1. `python dispatch_consumer.py`
2. `python traffic_speed_producer.py`
3. `python incident_demand_producer.py`
4. `python context_producer.py`
5. `python ambulance_producer.py`

The ambulance producer updates every 5 seconds and uses an accelerated
simulation clock. It reads the latest `final_multiplier` from MySQL, so a
traffic multiplier above 1.0 directly reduces the ambulance's effective
speed.

### What was added

- `ambulance_producer.py` — generates five live ambulance GPS streams.
- `hospitals` — five Delhi hospital markers.
- `ambulance_routes` — ordered route points for each ambulance.
- `ambulance_positions` — live/current and historical ambulance telemetry.
- Geomap layers for hospitals, five routes and live ambulance markers.
- `🚑 Live Ambulance Fleet` table with status, progress, remaining distance and ETA.

The Geomap uses OpenStreetMap as the base layer. Grafana's Geomap supports
real-time changing locations, fleet tracking, marker symbols/rotation and
route layers.

## Live reload configuration (final)

This final package is configured for the live ambulance demo:

- Grafana dashboard refresh: **5 seconds**.
- Ambulance producer GPS update interval: **5 seconds**.
- Grafana ambulance map layer selects the latest `position_id` for each ambulance.
- The live query returns `latitude`, `longitude`, `heading_deg`, and `event_time`, so the marker position and tooltip update with each dashboard refresh.
- The fleet table shows the latest GPS timestamp for every ambulance.

No second MySQL database is required. The dashboard uses the same `sda_smart_dispatch` database and `ambulance_positions` table used by the consumer.
