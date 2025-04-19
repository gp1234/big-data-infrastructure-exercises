import os
import json
from io import BytesIO
from datetime import datetime, timedelta
from contextlib import contextmanager

import boto3
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2 import pool

S3_BUCKET = os.getenv("BDI_S3_BUCKET", "bdi-aircraft-gio-s3")
PG_HOST = os.getenv("BDI_DB_HOST", "localhost")
PG_PORT = int(os.getenv("BDI_DB_PORT", "5432"))
PG_DBNAME = os.getenv("BDI_DB_DBNAME", "postgres")
PG_USER = os.getenv("BDI_DB_USERNAME", "postgres")
PG_PASSWORD = os.getenv("BDI_DB_PASSWORD", "postgres")

def get_connection_pool():
    return pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        dbname=PG_DBNAME,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
    )

@contextmanager
def get_db_conn():
    pool_conn = get_connection_pool()
    conn = pool_conn.getconn()
    try:
        yield conn
    finally:
        pool_conn.putconn(conn)

def download_files(**context):
    execution_date = context["ds"]
    date_obj = datetime.strptime(execution_date, "%Y-%m-%d")
    if date_obj.day != 1:
        return

    base_url = f"https://samples.adsbexchange.com/readsb-hist/{date_obj.strftime('%Y/%m/%d')}/"
    s3 = boto3.client("s3")
    s3_prefix = f"raw/day={date_obj.strftime('%Y%m%d')}/"

    response = requests.get(base_url)
    if response.status_code != 200:
        print(f"[ERROR] Failed to access: {base_url}")
        return

    soup = BeautifulSoup(response.content, "html.parser")
    links = [a["href"] for a in soup.find_all("a") if a["href"].endswith(".json.gz")]
    print(f"[FOUND] {len(links)} files")

    for filename in links:
        s3_key = s3_prefix + filename
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
            print(f"[SKIP] Exists: {s3_key}")
            continue
        except s3.exceptions.ClientError:
            pass

        url = base_url + filename
        res = requests.get(url)
        if res.status_code == 200:
            s3.upload_fileobj(BytesIO(res.content), S3_BUCKET, s3_key)
            print(f"[S3] Uploaded: {s3_key}")
        else:
            print(f"[WARN] Failed: {url}")

def prepare_files(**context):
    execution_date = context["ds"]
    date_obj = datetime.strptime(execution_date, "%Y-%m-%d")
    if date_obj.day != 1:
        return

    s3 = boto3.client("s3")
    date_str = date_obj.strftime('%Y%m%d')
    raw_prefix = f"raw/day={date_str}/"
    prepared_key = f"prepared/day={date_str}/grouped_aircraft.json"

    try:
        s3.head_object(Bucket=S3_BUCKET, Key=prepared_key)
        print(f"[SKIP] Prepared file exists: {prepared_key}")
        return
    except s3.exceptions.ClientError:
        pass

    tmp_dir = "/tmp/raw_aircraft"
    os.makedirs(tmp_dir, exist_ok=True)

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=raw_prefix)

    all_aircraft = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json.gz"):
                continue
            data = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
            local_file = os.path.join(tmp_dir, os.path.basename(key).replace(".gz", ""))
            with open(local_file, "wb") as f:
                f.write(data)
            with open(local_file) as f:
                content = json.load(f)
                if "aircraft" in content:
                    for a in content["aircraft"]:
                        all_aircraft.append({
                            "icao": a.get("hex", ""),
                            "registration": a.get("r", ""),
                            "type": a.get("t", ""),
                            "lat": a.get("lat", ""),
                            "lon": a.get("lon", ""),
                            "timestamp": a.get("seen_pos", ""),
                            "max_alt_baro": a.get("alt_baro", ""),
                            "max_ground_speed": a.get("gs", ""),
                            "had_emergency": a.get("emergency", "").lower() != "none" if a.get("emergency") else False,
                            "file_name": os.path.basename(local_file),
                        })

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS traces (
                    id SERIAL PRIMARY KEY,
                    icao TEXT NOT NULL,
                    registration TEXT,
                    type TEXT,
                    lat DOUBLE PRECISION,
                    lon DOUBLE PRECISION,
                    timestamp TEXT,
                    max_alt_baro NUMERIC,
                    max_ground_speed NUMERIC,
                    had_emergency BOOLEAN,
                    file_name TEXT
                );
            """)
            for row in all_aircraft:
                cur.execute("""
                    INSERT INTO traces (
                        icao, registration, type, lat, lon,
                        timestamp, max_alt_baro, max_ground_speed,
                        had_emergency, file_name
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, tuple(row.values()))
            conn.commit()

    grouped = {}
    for ac in all_aircraft:
        grouped.setdefault(ac["icao"], []).append({k: v for k, v in ac.items() if k != "icao"})

    with open("/tmp/grouped_aircraft.json", "w") as f:
        json.dump([{"icao": k, "traces": v} for k, v in grouped.items()], f, indent=2)

    s3.upload_file("/tmp/grouped_aircraft.json", S3_BUCKET, prepared_key)
    print(f"[S3] Uploaded grouped: {prepared_key}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="readsb_hist_etl",
    default_args=default_args,
    description="ETL for ADS-B readsb-hist",
    schedule="@monthly",
    start_date=datetime(2023, 11, 1),
    end_date=datetime(2024, 11, 1),
    catchup=True,
    max_active_runs=1,
    tags=["readsb", "etl", "aviation"],
) as dag:
    download = PythonOperator(
        task_id="download_readsb_files",
        python_callable=download_files,
    )
    prepare = PythonOperator(
        task_id="prepare_readsb_files",
        python_callable=prepare_files,
    )
    download >> prepare