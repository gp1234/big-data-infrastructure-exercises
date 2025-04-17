import os
import json
from io import BytesIO
from datetime import datetime, timedelta
from contextlib import contextmanager

import boto3
from psycopg2 import pool
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from bs4 import BeautifulSoup

S3_BUCKET = os.environ["S3_BUCKET"]
PG_HOST = os.environ["PG_HOST"]
PG_PORT = os.environ.get("PG_PORT", 5432)
PG_DBNAME = os.environ["PG_DBNAME"]
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]

connection_pool = pool.ThreadedConnectionPool(
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
    conn = connection_pool.getconn()
    try:
        yield conn
    finally:
        connection_pool.putconn(conn)

def ensure_traces_table():
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
                    had_emergency BOOLEAN DEFAULT FALSE
                );
                CREATE INDEX IF NOT EXISTS idx_traces_icao ON traces (icao);
                CREATE INDEX IF NOT EXISTS idx_traces_timestamp ON traces (timestamp);
                CREATE INDEX IF NOT EXISTS idx_traces_alt_speed ON traces (max_alt_baro, max_ground_speed);
            """)
            conn.commit()

def download_files(**context):
    execution_date = context["ds"]
    date_obj = datetime.strptime(execution_date, "%Y-%m-%d")

    if date_obj.day != 1:
        print(f"Skipping {execution_date} — not 1st of the month")
        return

    base_url = f"https://samples.adsbexchange.com/readsb-hist/{date_obj.strftime('%Y/%m/%d')}/"
    s3_client = boto3.client("s3")
    s3_prefix = f"raw/day={date_obj.strftime('%Y%m%d')}/"
    response = requests.get(base_url)
    if response.status_code != 200:
        print(f"Failed to access {base_url}")

    for i in range(100):
        soup = BeautifulSoup(response.content, 'html.parser')
        links = [a['href'] for a in soup.find_all('a') if a['href'].endswith('.json.gz')]
        
        if i >= len(links):
            break
            
        filename = links[i]
        url = base_url + filename
        try:
            response = boto3.client("s3")._endpoint.http_session.get(url)
            if response.status_code == 200:
                s3_key = s3_prefix + filename
                s3_client.upload_fileobj(BytesIO(response.content), S3_BUCKET, s3_key)
        except Exception as e:
            print(f"Failed to download or upload {filename}: {e}")

def prepare_files(**context):
    execution_date = context["ds"]
    date_obj = datetime.strptime(execution_date, "%Y-%m-%d")

    if date_obj.day != 1:
        print(f"Skipping {execution_date} — not 1st of the month")
        return

    s3_client = boto3.client("s3")
    prefix = f"raw/day={date_obj.strftime('%Y%m%d')}/"

    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)

    ensure_traces_table()
    
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            for page in page_iterator:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    try:
                        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
                        data = json.load(BytesIO(response["Body"].read()))

                        for entry in data.get("aircraft", []):
                            icao = entry.get("hex")
                            cur.execute("""
                                INSERT INTO traces
                                (icao, lat, lon, timestamp, max_alt_baro, max_ground_speed, had_emergency, registration, type)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                icao,
                                entry.get("lat", 0.0) if isinstance(entry.get("lat"), (int, float)) else 0.0,
                                entry.get("lon", 0.0) if isinstance(entry.get("lon"), (int, float)) else 0.0,
                                entry.get("seen_pos", ""),
                                entry.get("alt_baro", 0.0) if isinstance(entry.get("alt_baro"), float) else 0.0,
                                entry.get("gs", 0.0) if isinstance(entry.get("gs"), float) else 0.0,
                                entry.get("alert") == 1,
                                entry.get("r", None),
                                entry.get("t", None)
                            ))
                    except Exception as e:
                        print(f"Error processing {key}: {e}")

            conn.commit()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="readsb_hist_etl",
    default_args=default_args,
    description="Download and prepare readsb-hist (split + idempotent)",
    schedule_interval="@daily",
    start_date=datetime(2023, 11, 1),
    end_date=datetime(2024, 11, 1),
    catchup=True,
    max_active_runs=1,
    tags=["readsb", "etl", "s8", "aviation"],
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
