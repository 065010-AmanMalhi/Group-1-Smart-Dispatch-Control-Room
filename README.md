# 🚑 Smart Dispatch Control Room

### Real-Time Emergency Dispatch Intelligence for Delhi

![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Streaming-Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)
![MySQL](https://img.shields.io/badge/Database-MySQL-4479A1?logo=mysql&logoColor=white)
![Grafana](https://img.shields.io/badge/Dashboard-Grafana-F46800?logo=grafana&logoColor=white)
![Project](https://img.shields.io/badge/Project-Academic%20Prototype-6C5CE7)
![Data](https://img.shields.io/badge/Data-Synthetic-orange)

> **From streaming events to explainable emergency-response insights.**

Smart Dispatch Control Room is a Streaming Data Analytics project that simulates how traffic, incidents, weather/road conditions and ambulance movement can affect emergency-response ETAs across five Delhi zones. Events stream through **Apache Kafka**, are processed with **Python**, stored in **MySQL**, and visualised in **Grafana**.

> ⚠️ **Academic simulation only:** Traffic, incident, weather/road-event readings and ambulance telemetry are synthetic. Routes and coordinates are illustrative—not live GPS or turn-by-turn navigation. This prototype must not be used for real emergency-dispatch decisions.

---

## 📸 Dashboard screenshots

These are **actual Grafana screenshots captured from the project presentation**, not mockups.

### 1. Live control-room overview

KPIs, live dispatch risk map, and zone risk matrix. The dashboard is configured to refresh every five seconds.

<img width="1600" height="900" alt="ds-1 sda" src="https://github.com/user-attachments/assets/80cd613a-1be8-4ddf-85a7-fac2fee55e2e" />


### 2. ETA and delay-driver analysis

Baseline versus live ETA, factor contributions, ETA multiplier timeline, and traffic speed trends.

<img width="1600" height="900" alt="ds-2 sda" src="https://github.com/user-attachments/assets/af47ebed-b28e-4111-8650-f5b117600153" />


### 3. Explainability and current risk ranking

Shows the input factors, final multiplier, live ETA, and current risk ranking by zone.

<img width="1600" height="900" alt="ds-2 sda" src="https://github.com/user-attachments/assets/027b8661-36fe-4535-a701-7c52bd0f930b" />


<img width="1600" height="900" alt="ds-3 sda" src="https://github.com/user-attachments/assets/f6f6dbef-237b-4581-a640-0146c65c0dbd" />



### 4. Alert, audit and fleet views

The full Grafana dashboard also includes alert-tier distribution, alert event history, dispatch audit records, and simulated ambulance fleet panels.

<img width="1600" height="900" alt="ds-5 sda" src="https://github.com/user-attachments/assets/76529c02-4b37-4b3e-a942-95b6ac672123" />


---

## ✨ What's inside

The project combines four event streams into one operational view.

| Component | What it does |
|---|---|
| 🚦 Traffic stream | Publishes synthetic zone-level traffic-speed events |
| 🚨 Incident stream | Publishes incident-demand and severity events |
| 🌦️ Zone context stream | Publishes weather and road-event context |
| 🚑 Ambulance stream | Publishes simulated ambulance-position updates |
| 🧠 Dispatch consumer | Calculates explainable ETA multipliers, risk tiers and alert transitions |
| 🗄️ MySQL | Stores decisions, alerts, route information and ambulance telemetry |
| 📊 Grafana | Displays KPIs, zone risk, ETA comparisons, alert history and the simulated fleet |

## 🗺️ Zones covered

- Connaught Place
- AIIMS Corridor
- Ring Road South
- Dwarka Expressway
- Nehru Place

## 🧱 Architecture

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

## ⚙️ Technology stack

| Component | Purpose |
|---|---|
| Python | Synthetic producers, stream consumption and dispatch calculations |
| Apache Kafka | Event streaming across four topics |
| MySQL | Persistent decisions, alerts and fleet telemetry |
| Grafana | Live dashboard and map visualisation |
| Docker | Recommended for running supporting services locally |

## 📂 Key files

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
| `screenshots/` | Actual dashboard screenshots embedded above |

## 🚀 Setup

### Prerequisites

- Python 3.10+
- A running Kafka broker (and Zookeeper if required by your Kafka distribution)
- MySQL with permission to create the project database and tables
- Grafana with network access to MySQL

This repository does not include a Docker Compose file for provisioning infrastructure. Start Kafka and MySQL before running the Python pipeline.

### 1. Clone and install

```bash
git clone https://github.com/065010-AmanMalhi/Group-1-Smart-Dispatch-Control-Room.git
cd Group-1-Smart-Dispatch-Control-Room
python -m venv .venv
```

**Windows PowerShell:**

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
notepad .env
```

**macOS/Linux:**

```bash
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Set the MySQL username/password and confirm `KAFKA_BOOTSTRAP_SERVERS`, `MYSQL_HOST` and `MYSQL_PORT` in `.env`. Never commit `.env` or real credentials. The consumer creates the `sda_smart_dispatch` database and required tables at startup.

### 2. Generate synthetic data

```bash
python generate_data.py
```

### 3. Start the consumer

```bash
python dispatch_consumer.py
```

### 4. Start the producers

Run each in a separate terminal with the virtual environment activated:

```bash
python traffic_speed_producer.py
python incident_demand_producer.py
python context_producer.py
python ambulance_producer.py
```

Start the consumer before the producers. Stop processes with **Ctrl+C**.

## 📊 Import the Grafana dashboard

1. Open Grafana (often `http://localhost:3000`; use your configured port).
2. Configure a MySQL data source for database `sda_smart_dispatch`.
3. Select **Dashboards → Import**.
4. Upload `Smart_Dispatch_Control_Room_Continuous.json`.
5. Map the dashboard's MySQL input to your configured data source.
6. Select **Last 5 minutes** and set refresh to **5s**.

The dashboard export contains 22 panels: KPI cards, live risk map, zone risk matrix, baseline-versus-live ETA, factor contribution, timelines, alert history, audit log, explainability, risk ranking and live ambulance fleet.

## 🧮 Dispatch logic

The configured factor weights are:

- Speed-drop weight: **0.40**
- Incident-severity weight: **0.35**
- Weather weight: **0.15**
- Road-event weight: **0.30**

The displayed ETA is:

```text
live_eta_min = baseline_eta_min × final_multiplier
```

The configured WARNING threshold begins at multiplier **1.30**; CRITICAL begins at **1.55**. The consumer records tier transitions to the alert log. These values are scenario parameters, not validated real-world emergency dispatch rules.

## 🗄️ MySQL tables

- `dispatch_decisions` — decision factors, multiplier, ETA and reason
- `alerts_log` — alert-tier transitions
- `hospitals` — simulated hospital markers
- `ambulance_routes` — predefined route waypoints
- `ambulance_positions` — synthetic fleet telemetry

## 👥 Team

- **Akshit Kansal** — 065008
- **Aman Malhi** — 065010
- **Kavya Makkar** — 065031
- **Mayank Jha** — 065036

## ⚠️ Limitations

- All traffic, incident, context and ambulance data is synthetic.
- Routes are illustrative and are not live road-network navigation.
- There is no integration with live GPS, traffic, weather or emergency-service systems.
- Scoring weights and alert thresholds have not been calibrated against historical response outcomes.
- A real deployment would require validated feeds, security controls, robust failure handling, and expert validation.

## 📄 License

No license is currently specified. Add a license file if you intend others to reuse this project.
