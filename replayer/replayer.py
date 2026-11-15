import json, os, time
from datetime import datetime, timezone, timedelta
import requests
import psycopg2

FEED = os.getenv("STATION_FEED_URL", "https://gbfs.citibikenyc.com/gbfs/en/station_status.json")
INTERVAL = max(10, int(os.getenv("POLL_SECONDS", "60")))

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "bikeflow")
DB_USER = os.getenv("POSTGRES_USER", "bikeflow")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "change_me_locally")

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=5432
    )

def main():
    session = requests.Session()
    while True:
        started = time.monotonic()
        try:
            response = session.get(FEED, timeout=25, headers={"User-Agent": "BikeFlow-DEPI-Student-Project/1.0"})
            response.raise_for_status()
            stations = response.json()["data"]["stations"]
            
            # ضبط وقت النافذة الحالية ليكون في الوقت الحقيقي تماماً لكي تقرأه الداشبورد
            now = datetime.now(timezone.utc)
            window_start = now - timedelta(minutes=5)
            
            conn = get_db_connection()
            cur = conn.cursor()
            
            # تفريغ البيانات القديمة لضمان تحديث النافذة الحية للداشبورد فوراً
            cur.execute("TRUNCATE TABLE live_aggregates;")
            
            for station in stations:
                station_id = str(station["station_id"])
                bikes = float(station.get("num_bikes_available", 0))
                docks = float(station.get("num_docks_available", 0))
                
                cur.execute("""
                    INSERT INTO live_aggregates (window_start, window_end, station_id, avg_bikes, min_bikes, max_bikes, event_count)
                    VALUES (%s, %s, %s, %s, %s, %s, 1);
                """, (window_start, now, station_id, bikes, bikes, docks))
            
            conn.commit()
            cur.close()
            conn.close()
            print(f"Refreshed live_aggregates with {len(stations)} stations at {now.isoformat()}", flush=True)
            
        except Exception as exc:
            print(f"Feed/database error: {exc}", flush=True)
            
        time.sleep(max(0, INTERVAL - (time.monotonic() - started)))

if __name__ == "__main__":
    main()