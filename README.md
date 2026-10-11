# Smart Dispatch Control Room
### Real-Time Emergency Dispatch Intelligence for Delhi

A Streaming Data Analytics project that simulates how traffic, incidents, weather/road conditions and ambulance movement can affect emergency-response ETAs across five Delhi zones. Events stream through **Apache Kafka**, are processed with **Python**, persisted in **MySQL**, and visualised in **Grafana**.

> **Academic simulation:** Traffic, incident, weather/road-event readings and ambulance telemetry are synthetic. Routes and coordinates are illustrative—not live GPS or turn-by-turn navigation. This prototype must not be used for real emergency-dispatch decisions.

## Project overview

- **Domain:** Emergency services / public safety
- **Pipeline:** Kafka producers → Python consumer and dispatch logic → MySQL → Grafana
- **Zones:** Connaught Place, AIIMS Corridor, Ring Road South, Dwarka Expressway and Nehru Place
- **Dashboard refresh:** 5 seconds
- **Outputs:** Explainable zone-level ETA multipliers, risk tiers, alert history and simulated ambulance positions

## Architecture

```text
Synthetic scenario data
  ├── traffic_speed_producer.py ───> traffic-speed-sc ─────┐
  ├── incident_demand_producer.py ─> incident-demand-sc ───┤
  ├── context_producer.py ─────────> zone-context-sc ──────┼──> dispatch_consumer.py
  └── ambulance_producer.py ───────> ambulance-position-sc ┘          │
                                                                      ▼
                                                        MySQL: sda_smart_dispatch
                                                                      │
                                                                      ▼
                                                        Grafana operations dashboard
```

The consumer listens to all four Kafka topics, maintains the latest zone readings, calculates and smooths a delay multiplier, records decisions and alert transitions, and stores synthetic ambulance telemetry for visualisation.

## Features

- Four Kafka streams: traffic speed, incident demand, weather/road context and simulated ambulance positions.
- Five-zone monitoring with configurable baseline speeds and ETAs.
- Explainable delay factors: speed drop, incident severity, rain intensity and road-event severity.
- Bounded ETA multiplier and smoothing to limit abrupt changes.
- **NORMAL**, **WARNING** and **CRITICAL** alert tiers, with tier changes written to an alert log.
- MySQL tables for decisions, alerts, hospitals, routes and ambulance positions.
- Grafana dashboard with **22 panels**, including KPI cards, risk map, zone risk matrix, ETA comparisons, factor contributions, time series, alert history, audit log, explainability and live ambulance fleet.
- Scripted calm → incident → recovery scenario for reproducible demonstrations.

## Technology stack

| Component | Purpose |
|---|---|
| Python | Synthetic producers, stream consumption and dispatch calculations |
| Apache Kafka | Event streaming across four topics |
| MySQL | Persistent decisions, alerts and fleet telemetry |
| Grafana | Live dashboard and map visualisation |
| Docker | Recommended for running supporting services locally |

## Key files

| File | Purpose |
|---|---|
| `config.py` | Kafka/MySQL settings, zones, weights, thresholds and scenario timing |
| `.env.example` | Safe template for local environment settings |
| `requirements.txt` | Python dependencies |
| `generate_data.py` | Creates synthetic scenario CSVs |
| `traffic_speed_producer.py` | Publishes traffic-speed events |
| `incident_demand_producer.py` | Publishes incident-demand events |
| `context_producer.py` | Publishes weather/road-context events |
| `ambulance_producer.py` | Publishes synthetic ambulance positions and route telemetry |
| `dispatch_consumer.py` | Consumes streams, calculates decisions and writes MySQL records |
| `traffic_speed.csv`, `incident_demand.csv`, `zone_context.csv` | Synthetic input data |
| `Smart_Dispatch_Control_Room_Continuous.json` | Grafana dashboard export |

## Prerequisites

- Python 3.10 or later
- A running Kafka broker (plus Zookeeper if required by your Kafka distribution)
- MySQL with a user permitted to create the project database and tables
- Grafana with network access to the same MySQL instance
- Docker Desktop or equivalent is recommended for the supporting services

This repository does **not** include a Docker Compose file to provision infrastructure. Start Kafka and MySQL before running the Python pipeline, and ensure Grafana can connect to that MySQL instance.

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/065010-AmanMalhi/Group-1-Smart-Dispatch-Control-Room.git
cd Group-1-Smart-Dispatch-Control-Room
```

### 2. Create a virtual environment and install dependencies

**Windows PowerShell:**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

**macOS/Linux:**

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Configure environment variables

Copy `.env.example` to `.env` and edit the values for your environment. The `.env` file is excluded by `.gitignore`; **never commit real passwords or API credentials**.

```powershell
# Windows PowerShell
Copy-Item .env.example .env
notepad .env
```

At minimum, set the correct MySQL username/password and check `KAFKA_BOOTSTRAP_SERVERS`, `MYSQL_HOST` and `MYSQL_PORT`. Hostnames depend on where Python runs: host-run Python commonly uses `localhost`; containers on the same Docker network commonly use a service/container name.

The consumer creates the `sda_smart_dispatch` database and required tables at startup. The configured MySQL user needs sufficient privileges to do this.

### 4. Generate synthetic input data

```bash
python generate_data.py
```

The generator writes `traffic_speed.csv`, `incident_demand.csv` and `zone_context.csv`. The scripted scenario lasts approximately 75 seconds and includes calm, incident and recovery phases. Example events include a major accident on Ring Road South, heavy rain in the AIIMS Corridor, and VIP movement/road closure in Connaught Place. Dwarka Expressway and Nehru Place act as calmer comparison zones.

### 5. Run the pipeline

Start the consumer first, then run each producer in a separate terminal. Activate the same virtual environment in each terminal.

| Terminal | Command | Stream |
|---|---|---|
| 1 | `python dispatch_consumer.py` | Consumes all four topics and writes MySQL |
| 2 | `python traffic_speed_producer.py` | Traffic speed |
| 3 | `python incident_demand_producer.py` | Incident demand |
| 4 | `python context_producer.py` | Weather and road events |
| 5 | `python ambulance_producer.py` | Synthetic ambulance positions |

Wait until the consumer reports that it is listening before starting the producers. Scenario producers replay generated data continuously; the ambulance producer publishes position updates on its configured interval. Stop processes with **Ctrl+C**.

## Grafana dashboard

1. Open your Grafana instance. The default local address is often `http://localhost:3000`; use the port mapped by your own setup.
2. Add a **MySQL** data source pointing to the MySQL service and database `sda_smart_dispatch`.
3. Confirm Grafana can reach MySQL using the host/container network address and credentials appropriate to your environment.
4. Navigate to **Dashboards → Import**.
5. Upload **`Smart_Dispatch_Control_Room_Continuous.json`**.
6. Map the dashboard's **MySQL** input to the data source you configured.
7. Set the time range to **Last 5 minutes** and confirm refresh is **5s**.

The dashboard export contains **22 panels**, including:

- Critical alerts, zones at risk, average live ETA, maximum ETA delay, average speed and multiplier KPIs
- Active incidents and zones monitored
- Live dispatch risk map and zone risk matrix
- Baseline-versus-live ETA and delay-driver contributions
- ETA multiplier, traffic speed and ETA-delay time series
- Alert-tier distribution and alert event log
- Dispatch audit log, explainable factor breakdown and current risk ranking
- Live ambulance fleet table and map

If Grafana cannot connect to MySQL, verify the hostname from Grafana's runtime environment, database name, credentials and network configuration. A hostname that works from your Windows terminal may not work from inside a Grafana container.

## Dispatch logic

The consumer combines current zone readings using configurable weights in `config.py`:

- Speed-drop weight: **0.40**
- Incident-severity weight: **0.35**
- Weather weight: **0.15**
- Road-event weight: **0.30**

The raw result is bounded by the configured minimum and maximum multiplier. A smoothing limit constrains how much the final multiplier can change per decision cycle. The displayed ETA is calculated as:

```text
live_eta_min = baseline_eta_min × final_multiplier
```

Alert thresholds are configured in `config.py`: WARNING begins at a multiplier of **1.30** and CRITICAL at **1.55**. When a zone changes severity tier, the consumer records an alert event rather than duplicating an alert every cycle.

These weights and thresholds are scenario parameters for an academic prototype, not medically validated dispatch rules.

## Database tables

The pipeline creates and uses these tables in `sda_smart_dispatch`:

- `dispatch_decisions` — input factors, multiplier, ETA and decision reason
- `alerts_log` — changes in alert severity
- `hospitals` — hospital markers used in the simulation
- `ambulance_routes` — ordered route points for the simulated fleet
- `ambulance_positions` — current and historical synthetic position telemetry

Optional sanity check (replace the container name and MySQL username for your environment; the command prompts for a password):

```bash
docker exec -it <mysql-container-name> mysql -u<mysql-user> -p -e "SELECT zone, MAX(final_multiplier) AS peak, MAX(live_eta_min) AS worst_eta FROM sda_smart_dispatch.dispatch_decisions GROUP BY zone ORDER BY peak DESC;"
```

## Troubleshooting

| Symptom | Checks |
|---|---|
| Python cannot connect to Kafka | Confirm the broker is running and `KAFKA_BOOTSTRAP_SERVERS` is reachable from the Python process |
| No new MySQL rows | Start the consumer first, then producers; verify credentials and consumer logs |
| Grafana panels are empty | Check the selected MySQL data source, database, time range and whether producers are running |
| Grafana cannot connect to MySQL | Use a hostname reachable from Grafana; verify Docker networking and credentials |
| Kafka import error | Activate the project virtual environment and run `pip install -r requirements.txt` |
| No simulated ambulance movement | Start `ambulance_producer.py`, then verify its Kafka topic, MySQL connection and consumer logs |

## Team

- **Akshit Kansal** — 065008
- **Aman Malhi** — 065010
- **Kavya Makkar** — 065031
- **Mayank Jha** — 065036

## Limitations and future scope

- Event and ambulance telemetry are synthetic; there is no live traffic, weather, dispatch-system or GPS integration.
- Routes are illustrative waypoint sequences, not road-network navigation.
- Scoring weights and alert thresholds have not been calibrated against historical emergency-response outcomes.
- A production version would require validated data feeds, robust failure handling, access control, monitoring, privacy safeguards, security review and validation with emergency-service experts.

## License

No license is currently specified. Add a license file if you intend others to reuse this project.
