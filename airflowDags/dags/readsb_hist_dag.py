import os
import json
import gzip
from io import BytesIO
from datetime import datetime, timedelta
from contextlib import contextmanager

import boto3
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from psycopg2 import pool

def get_config(key, default=None):
    return os.environ.get(key) or Variable.get(key, default_var=default)

S3_BUCKET = get_config("BDI_S3_BUCKET", "bdi-aircraft-gio-s3")
PG_HOST = get_config("BDI_DB_HOST", "localhost")
PG_PORT = int(get_config("BDI_DB_PORT", "5437"))
PG_DBNAME = get_config("BDI_DB_DBNAME", "postgres")
PG_USER = get_config("BDI_DB_USERNAME", "postgres")
PG_PASSWORD = get_config("BDI_DB_PASSWORD", "postgres")

def get_connection_pool():
    return pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        dbname=PG_DBNAME,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )

@contextmanager
def get_db_conn():
    pool = get_connection_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)

def download_files(**context):
    execution_date = context["ds"]
    date_obj = datetime.strptime(execution_date, "%Y-%m-%d")
    if date_obj.day != 1:
        print(f"[SKIP] {execution_date} is not the 1st of the month.")
        return

    base_url = f"https://samples.adsbexchange.com/readsb-hist/{date_obj.strftime('%Y/%m/%d')}/"
    s3 = boto3.client("s3")
    s3_prefix = f"raw/day={date_obj.strftime('%Y%m%d')}/"

    print(f"[START] Accessing: {base_url}")
    response = requests.get(base_url)
    if response.status_code != 200:
        print(f"[ERROR] Failed to access index page: {base_url}")
        return

    soup = BeautifulSoup(response.content, "html.parser")
    links = [a["href"] for a in soup.find_all("a") if a["href"].endswith(".json.gz")]
    print(f"[FOUND] {len(links)} files in index.")

    for idx, filename in enumerate(links):
        url = base_url + filename
        print(f"[DOWNLOAD] {idx+1}/{len(links)} - {filename}")
        try:
            res = requests.get(url)
            if res.status_code == 200:
                s3_key = s3_prefix + filename
                s3.upload_fileobj(BytesIO(res.content), S3_BUCKET, s3_key)
                print(f"[S3] Uploaded to s3://{S3_BUCKET}/{s3_key}")
            else:
                print(f"[WARN] Failed to fetch file: {url}")
        except Exception as e:
            print(f"[ERROR] Failed {filename}: {e}")

def prepare_files(**context):
    execution_date = context["ds"]
    date_obj = datetime.strptime(execution_date, "%Y-%m-%d")
    if date_obj.day != 1:
        print(f"[SKIP] {execution_date} is not the 1st of the month.")
        return

    s3 = boto3.client("s3")
    raw_prefix = f"raw/day={date_obj.strftime('%Y%m%d')}/"
    prepared_key = f"prepared/day={date_obj.strftime('%Y%m%d')}/grouped_aircraft.json"
    tmp_raw_dir = "/tmp/raw_aircraft"
    os.makedirs(tmp_raw_dir, exist_ok=True)

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=raw_prefix)

    print(f"[S3] Listing raw files from: {raw_prefix}")
    file_count = 0
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json.gz"):
                continue
            file_count += 1
            raw_data = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
            local_file_path = os.path.join(tmp_raw_dir, os.path.basename(key).replace(".gz", ""))
            with open(local_file_path, "wb") as f:
                f.write(gzip.decompress(raw_data))
            print(f"[DECOMPRESS] {key} → {local_file_path}")
    print(f"[INFO] Total raw files processed: {file_count}")

    all_transformed_aircraft = []
    for file_name in os.listdir(tmp_raw_dir):
        if file_name.endswith(".json"):
            file_path = os.path.join(tmp_raw_dir, file_name)
            with open(file_path) as file:
                data = json.load(file)
            if "aircraft" in data:
                aircraft_data = data["aircraft"]
                transformed = [
                    {
                        "icao": entry.get("hex", ""),
                        "registration": entry.get("r", ""),
                        "type": entry.get("t", ""),
                        "lat": entry.get("lat", ""),
                        "lon": entry.get("lon", ""),
                        "timestamp": entry.get("seen_pos", ""),
                        "max_alt_baro": entry.get("alt_baro", ""),
                        "max_ground_speed": entry.get("gs", ""),
                        "had_emergency": (
                            entry.get("emergency", "").lower() != "none" if entry.get("emergency") else False
                        ),
                        "file_name": file_name,
                    }
                    for entry in aircraft_data
                ]
                all_transformed_aircraft.extend(transformed)
    print(f"[TRANSFORM] Total records prepared: {len(all_transformed_aircraft)}")

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
            for idx, row in enumerate(all_transformed_aircraft):
                cur.execute("""
                    INSERT INTO traces (
                        icao, registration, type, lat, lon,
                        timestamp, max_alt_baro, max_ground_speed,
                        had_emergency, file_name
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    row["icao"],
                    row["registration"],
                    row["type"],
                    row["lat"],
                    row["lon"],
                    row["timestamp"],
                    row["max_alt_baro"],
                    row["max_ground_speed"],
                    row["had_emergency"],
                    row["file_name"]
                ))
                if idx % 500 == 0:
                    print(f"[DB] Inserted {idx}/{len(all_transformed_aircraft)} rows...")
            conn.commit()
            print(f"[DB] All {len(all_transformed_aircraft)} rows inserted.")

    grouped_aircraft = {}
    for aircraft in all_transformed_aircraft:
        icao = aircraft["icao"]
        if icao not in grouped_aircraft:
            grouped_aircraft[icao] = []
        grouped_aircraft[icao].append({k: v for k, v in aircraft.items() if k != "icao"})

    result = [{"icao": icao, "traces": traces} for icao, traces in grouped_aircraft.items()]
    output_file_path = "/tmp/prepared_grouped.json"
    with open(output_file_path, "w") as f:
        json.dump(result, f, indent=2)

    s3.upload_file(output_file_path, S3_BUCKET, prepared_key)
    print(f"[S3] Uploaded grouped JSON to: s3://{S3_BUCKET}/{prepared_key}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="readsb_hist_etl",
    default_args=default_args,
    description="ETL for ADS-B readsb-hist: download raw → prepare → store & insert",
    schedule="@monthly",
    start_date=datetime(2023, 11, 1),
    end_date=datetime(2024, 11, 1),
    catchup=True,
    max_active_runs=1,
    tags=["readsb", "etl", "aviation"],
) as dag:

    download_task = PythonOperator(
        task_id="download_readsb_files",
        python_callable=download_files,
        provide_context=True,
    )

    prepare_task = PythonOperator(
        task_id="prepare_readsb_files",
        python_callable=prepare_files,
        provide_context=True,
    )

    download_task >> prepare_task