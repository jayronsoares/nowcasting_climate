# ingest.py
import os, requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import date
from dotenv import load_dotenv
from stations import STATIONS

load_dotenv()
conn = psycopg2.connect(os.getenv("TIMESCALE_URL"))

def fetch_inmet(station_id: str, start: str, end: str) -> list[dict]:
    url = f"https://apitempo.inmet.gov.br/estacao/{start}/{end}/{station_id}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json() or []

def parse_timestamp(row: dict) -> str | None:
    date_str = row.get("DT_MEDICAO", "")
    hour = (row.get("HR_MEDICAO") or "0000")[:4]
    if not date_str:
        return None
    return f"{date_str}T{hour[:2]}:{hour[2:]}:00+00:00"

def safe_float(val):
    try:
        return float(val) if val not in (None, "", "-9999", -9999) else None
    except (ValueError, TypeError):
        return None

def insert_batch(rows: list[dict], state: str, station_id: str, lat: float, lon: float):
    cur = conn.cursor()
    records = []
    for r in rows:
        ts = parse_timestamp(r)
        if not ts:
            continue
        records.append((
            state, station_id, lat, lon, None, ts,
            safe_float(r.get("CHUVA")),
            safe_float(r.get("TEM_INS")),
            safe_float(r.get("UMD_INS")),
            safe_float(r.get("PRE_INS")),
            safe_float(r.get("VEN_VEL")),
        ))
    if records:
        execute_values(cur, """
            INSERT INTO weather_observations
                (state_code, station_id, lat, lon, grid_id, ts,
                 rain_mm, temp_c, humidity, pressure, wind_speed)
            VALUES %s
            ON CONFLICT (station_id, ts) DO NOTHING
        """, records)
        conn.commit()
    cur.close()

def run(start: str = "2022-01-01"):
    end = date.today().isoformat()
    for state, stations in STATIONS.items():
        for (sid, lat, lon) in stations:
            try:
                rows = fetch_inmet(sid, start, end)
                insert_batch(rows, state, sid, lat, lon)
                print(f"✓ {state} / {sid} — {len(rows)} registros")
            except Exception as e:
                print(f"✗ {state} / {sid} — erro: {e}")

run("2022-01-01")