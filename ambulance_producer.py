"""
ambulance_producer.py
=====================
Synthetic live ambulance GPS stream for the Smart Dispatch Control Room.

The hospitals are real Delhi facilities listed by Delhi Government district
pages. The ambulance telemetry and incident destinations are synthetic for
academic/demo use. Each ambulance follows a fixed multi-waypoint path so the
Grafana Geomap can draw a route and move the ambulance marker along it.

Run after dispatch_consumer.py. Stop with Ctrl+C.
"""
import json, math, time
import mysql.connector
from kafka import KafkaProducer
from config import (
    KAFKA_BOOTSTRAP_SERVERS, TOPIC_AMBULANCE,
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE,
    AMBULANCE_UPDATE_SECONDS, AMBULANCE_SIM_TIME_SCALE,
)

# Coordinates are approximate simulation points for the named facilities /
# destinations. Hospital names and addresses are based on Delhi Government
# listings; route telemetry itself is synthetic.
HOSPITALS = {
    "AIIMS Hospital": {"lat": 28.5672, "lon": 77.2100, "zone": "AIIMS Corridor"},
    "Max Super Speciality Hospital, Patparganj": {"lat": 28.6209, "lon": 77.2940, "zone": "Ring Road South"},
    "Ram Manohar Lohia Hospital": {"lat": 28.6250, "lon": 77.1994, "zone": "Connaught Place"},
    "Fortis Hospital Shalimar Bagh": {"lat": 28.7055, "lon": 77.1593, "zone": "Ring Road South"},
    "Aakash Healthcare, Super Specialty Hospital": {"lat": 28.5789, "lon": 77.0717, "zone": "Dwarka Expressway"},
}

# Each path is intentionally multi-segment rather than a straight line. It
# approximates major Delhi corridors and gives the dashboard a convincing route
# shape without requiring an external routing API during the demo.
AMBULANCES = [
    {
        "ambulance_id": "AMB-101", "hospital": "AIIMS Hospital",
        "destination": "Synthetic incident: Connaught Place",
        "zone": "Connaught Place", "base_speed_kmph": 38,
        "waypoints": [(28.5672,77.2100),(28.5750,77.2102),(28.5900,77.2220),(28.6128,77.2295),(28.6255,77.2220),(28.6315,77.2167)],
    },
    {
        "ambulance_id": "AMB-102", "hospital": "Max Super Speciality Hospital, Patparganj",
        "destination": "Synthetic incident: Ring Road South", "zone": "Ring Road South", "base_speed_kmph": 42,
        "waypoints": [(28.6209,77.2940),(28.6245,77.2790),(28.6288,77.2550),(28.6280,77.2415),(28.6100,77.2400),(28.5850,77.2390),(28.5607,77.2385)],
    },
    {
        "ambulance_id": "AMB-103", "hospital": "Ram Manohar Lohia Hospital",
        "destination": "Synthetic incident: Nehru Place", "zone": "Nehru Place", "base_speed_kmph": 40,
        "waypoints": [(28.6250,77.1994),(28.6180,77.2090),(28.6128,77.2295),(28.5903,77.2228),(28.5740,77.2350),(28.5590,77.2450),(28.5491,77.2507)],
    },
    {
        "ambulance_id": "AMB-104", "hospital": "Fortis Hospital Shalimar Bagh",
        "destination": "Synthetic incident: AIIMS Corridor", "zone": "AIIMS Corridor", "base_speed_kmph": 43,
        "waypoints": [(28.7055,77.1593),(28.6900,77.1580),(28.6700,77.1570),(28.6490,77.1600),(28.6200,77.1640),(28.5905,77.1730),(28.5750,77.1940),(28.5672,77.2100)],
    },
    {
        "ambulance_id": "AMB-105", "hospital": "Aakash Healthcare, Super Specialty Hospital",
        "destination": "Synthetic incident: Dwarka Expressway", "zone": "Dwarka Expressway", "base_speed_kmph": 45,
        "waypoints": [(28.5789,77.0717),(28.5900,77.0680),(28.6000,77.0610),(28.6100,77.0540),(28.6200,77.0480),(28.6250,77.0450)],
    },
]

def haversine(a,b):
    lat1,lon1=a; lat2,lon2=b
    R=6371.0
    p1,p2=math.radians(lat1),math.radians(lat2)
    dp=math.radians(lat2-lat1); dl=math.radians(lon2-lon1)
    x=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(x))

def build_route(waypoints, points_per_segment=18):
    pts=[]
    for i in range(len(waypoints)-1):
        a,b=waypoints[i],waypoints[i+1]
        n=points_per_segment
        for j in range(n):
            t=j/n
            pts.append((a[0]+(b[0]-a[0])*t, a[1]+(b[1]-a[1])*t))
    pts.append(waypoints[-1])
    # cumulative distance
    out=[]; total=0.0
    prev=None
    for idx,p in enumerate(pts):
        if prev is not None: total += haversine(prev,p)
        out.append({"seq":idx,"lat":p[0],"lon":p[1],"distance_km":total})
        prev=p
    return out

ROUTES={a["ambulance_id"]:build_route(a["waypoints"]) for a in AMBULANCES}

def db():
    return mysql.connector.connect(host=MYSQL_HOST,port=MYSQL_PORT,user=MYSQL_USER,password=MYSQL_PASSWORD,database=MYSQL_DATABASE)

def setup_tables(conn):
    cur=conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS ambulance_routes (
        route_id INT AUTO_INCREMENT PRIMARY KEY,
        ambulance_id VARCHAR(20) NOT NULL,
        route_seq INT NOT NULL,
        latitude DOUBLE NOT NULL,
        longitude DOUBLE NOT NULL,
        cumulative_distance_km DOUBLE NOT NULL,
        UNIQUE KEY uq_route (ambulance_id, route_seq)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS ambulance_positions (
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
    )""")
    for a in AMBULANCES:
        cur.execute("DELETE FROM ambulance_routes WHERE ambulance_id=%s",(a["ambulance_id"],))
        cur.executemany("INSERT INTO ambulance_routes (ambulance_id,route_seq,latitude,longitude,cumulative_distance_km) VALUES (%s,%s,%s,%s,%s)",[(a["ambulance_id"],p["seq"],p["lat"],p["lon"],p["distance_km"]) for p in ROUTES[a["ambulance_id"]]])
    conn.commit(); cur.close()

def latest_multiplier(conn, zone):
    cur=conn.cursor()
    cur.execute("SELECT final_multiplier FROM dispatch_decisions WHERE zone=%s ORDER BY decision_id DESC LIMIT 1",(zone,))
    row=cur.fetchone(); cur.close()
    return float(row[0]) if row else 1.0

def point_at_distance(route, distance):
    if distance <= 0: return route[0]
    if distance >= route[-1]["distance_km"]: return route[-1]
    for i in range(1,len(route)):
        if route[i]["distance_km"] >= distance:
            a,b=route[i-1],route[i]
            span=b["distance_km"]-a["distance_km"]
            t=0 if span<=0 else (distance-a["distance_km"])/span
            return {"lat":a["lat"]+(b["lat"]-a["lat"])*t,"lon":a["lon"]+(b["lon"]-a["lon"])*t}
    return route[-1]

def main():
    conn=db(); setup_tables(conn)
    producer=KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,value_serializer=lambda v:json.dumps(v).encode("utf-8"))
    state={a["ambulance_id"]:0.0 for a in AMBULANCES}
    print(f"Ambulance producer: {len(AMBULANCES)} live vehicles, update every {AMBULANCE_UPDATE_SECONDS}s, sim scale {AMBULANCE_SIM_TIME_SCALE}x")
    print("Press Ctrl+C to stop.")
    try:
        while True:
            for a in AMBULANCES:
                aid=a["ambulance_id"]; route=ROUTES[aid]; total=route[-1]["distance_km"]
                mult=max(latest_multiplier(conn,a["zone"]),1.0)
                sim_hours=(AMBULANCE_UPDATE_SECONDS*AMBULANCE_SIM_TIME_SCALE)/3600.0
                state[aid] += (a["base_speed_kmph"]/mult)*sim_hours
                if state[aid] > total:
                    state[aid]=0.0
                pt=point_at_distance(route,state[aid]); progress=(state[aid]/total*100) if total else 100
                remaining=max(total-state[aid],0)
                effective=max(a["base_speed_kmph"]/mult,1)
                eta=remaining/effective*60
                if progress >= 99.5: status="ARRIVED"
                elif mult >= 1.55: status="CRITICAL DELAY"
                elif mult >= 1.30: status="DELAYED"
                else: status="EN ROUTE"
                payload={"ambulance_id":aid,"hospital":a["hospital"],"destination":a["destination"],"zone":a["zone"],"latitude":round(pt["lat"],6),"longitude":round(pt["lon"],6),"speed_kmph":round(effective,1),"route_progress_pct":round(progress,1),"remaining_distance_km":round(remaining,2),"eta_min":round(eta,1),"final_multiplier":round(mult,2),"status":status}
                producer.send(TOPIC_AMBULANCE,value=payload)
            producer.flush(); time.sleep(AMBULANCE_UPDATE_SECONDS)
    except KeyboardInterrupt: print("\nAmbulance producer stopped by user.")
    finally:
        producer.flush(); producer.close(); conn.close()

if __name__=="__main__": main()
