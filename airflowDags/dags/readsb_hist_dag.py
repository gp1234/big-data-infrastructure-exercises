import os
import json
import re
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

conn_pool = pool.ThreadedConnectionPool(
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
    conn = conn_pool.getconn()
    try:
        yield conn
    finally:
        conn_pool.putconn(conn)

def ensure_tables_exist(cursor):
    cursor.execute("""
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
        had_emergency BOOLEAN DEFAULT FALSE,
        day DATE
    );
    CREATE INDEX IF NOT EXISTS idx_traces_icao ON traces (icao);
    CREATE INDEX IF NOT EXISTS idx_traces_day ON traces (day);
    """)

def download_files(**context):
    execution_date = context["ds"]
    date_obj = datetime.strptime(execution_date, "%Y-%m-%d")
    if date_obj.day != 1:
        print(f"[SKIP] {execution_date} is not the 1st of the month.")
        return

    base_url = f"https://samples.adsbexchange.com/readsb-hist/{date_obj.strftime('%Y/%m/%d')}/"
    s3 = boto3.client("s3")
    s3_prefix = f"raw/day={date_obj.strftime('%Y%m%d')}/"

    response = requests.get(base_url)
    if response.status_code != 200:
        print(f"[ERROR] Failed to access index page: {base_url}")
        return

    soup = BeautifulSoup(response.content, "html.parser")
    links = [a["href"] for a in soup.find_all("a") if a["href"].endswith(".json.gz")]
    print(f"[FOUND] {len(links)} files in index.")

    for idx, filename in enumerate(links[:100]):
        s3_key = s3_prefix + filename
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
            print(f"[SKIP] Already exists in S3: {s3_key}")
            continue
        except s3.exceptions.ClientError:
            pass

        url = base_url + filename
        res = requests.get(url)
        if res.status_code == 200:
            s3.upload_fileobj(BytesIO(res.content), S3_BUCKET, s3_key)
            print(f"[S3] Uploaded to s3://{S3_BUCKET}/{s3_key}")
        else:
            print(f"[WARN] Failed to fetch file: {url}")

def prepare_files(**context):
    execution_date = context["ds"]
    date_obj = datetime.strptime(execution_date, "%Y-%m-%d")
    if date_obj.day != 1:
        print(f"[SKIP] {execution_date} is not the 1st of the month.")
        return

    s3 = boto3.client("s3")
    raw_prefix = f"raw/day={date_obj.strftime('%Y%m%d')}/"
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=raw_prefix)

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            ensure_tables_exist(cur)
            inserted_rows = 0
            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not key.endswith(".json.gz"):
                        continue
                    print(f"[PROCESS] Reading {key}")

                    day_match = re.search(r"day=(\d{8})", key)
                    day_val = datetime.strptime(day_match.group(1), "%Y%m%d").date() if day_match else None

                    try:
                        raw_data = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
                        data = json.loads(raw_data)
                    except Exception as e:
                        print(f"[ERROR] Failed to parse {key}: {e}")
                        continue

                    for entry in data.get("aircraft", []):
                        icao = entry.get("hex")
                        try:
                            cur.execute("""
                                INSERT INTO traces
                                (icao, lat, lon, timestamp, max_alt_baro, max_ground_speed, had_emergency, registration, type, day)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """, (
                                icao,
                                entry.get("lat", 0.0) if isinstance(entry.get("lat"), (int, float)) else 0.0,
                                entry.get("lon", 0.0) if isinstance(entry.get("lon"), (int, float)) else 0.0,
                                entry.get("seen_pos", ""),
                                entry.get("alt_baro", 0.0) if isinstance(entry.get("alt_baro"), (float,)) else 0.0,
                                entry.get("gs", 0.0) if isinstance(entry.get("gs"), (float,)) else 0.0,
                                entry.get("alert") == 1,
                                entry.get("r"),
                                entry.get("t"),
                                day_val
                            ))
                            inserted_rows += 1
                            if inserted_rows % 500 == 0:
                                print(f"[DB] Inserted {inserted_rows} rows...")
                                conn.commit()
                        except Exception as e:
                            print(f"[WARN] Skipping entry for {icao}: {e}")
            conn.commit()
            print(f"[DB] ✅ Final commit — total rows inserted: {inserted_rows}")

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
    )

    prepare_task = PythonOperator(
        task_id="prepare_readsb_files",
        python_callable=prepare_files,
    )

    download_task >> prepare_task
