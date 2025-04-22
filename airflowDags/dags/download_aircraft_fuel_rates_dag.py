import os
import json
from io import BytesIO
from datetime import datetime, timedelta
from contextlib import contextmanager

import boto3
import requests
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

def ensure_fuel_table():
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS aircraft_fuel_consumption (
                    aircraft_type TEXT PRIMARY KEY,
                    name TEXT,
                    galph DOUBLE PRECISION,
                    category TEXT,
                    source TEXT
                );
                """
            )
            conn.commit()

def download_and_process_fuel_rates(**context):
    url = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
    execution_date = context["ds"]
    date_str = execution_date.replace("-", "")
    s3_raw_key = f"raw/fuel_rates/day={date_str}/aircraft_type_fuel.json"
    s3_prepared_key = f"prepared/fuel_rates/day={date_str}/aircraft_type_fuel_prepared.json"

    s3 = boto3.client("s3")

    try:
        s3.head_object(Bucket=S3_BUCKET, Key=s3_prepared_key)
        print(f"[SKIP] Already exists in S3: {s3_prepared_key}")
        return
    except s3.exceptions.ClientError:
        pass

    response = requests.get(url)
    response.raise_for_status()
    content = response.content
    s3.put_object(Bucket=S3_BUCKET, Key=s3_raw_key, Body=content)
    print(f"[S3] Uploaded raw file to: s3://{S3_BUCKET}/{s3_raw_key}")

    raw_data = json.loads(content)
    prepared_data = [
        {
            "aircraft_type": aircraft_type,
            "name": details.get("name"),
            "galph": details.get("galph"),
            "category": details.get("category"),
            "source": details.get("source"),
        }
        for aircraft_type, details in raw_data.items()
    ]

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_prepared_key,
        Body=json.dumps(prepared_data, indent=2),
    )
    print(f"[S3] Uploaded prepared file to: s3://{S3_BUCKET}/{s3_prepared_key}")

    ensure_fuel_table()
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            for i, row in enumerate(prepared_data, start=1):
                cur.execute(
                    """
                    INSERT INTO aircraft_fuel_consumption (
                        aircraft_type, name, galph, category, source
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (aircraft_type) DO UPDATE SET
                        name = EXCLUDED.name,
                        galph = EXCLUDED.galph,
                        category = EXCLUDED.category,
                        source = EXCLUDED.source;
                    """,
                    (
                        row["aircraft_type"],
                        row["name"],
                        row["galph"],
                        row["category"],
                        row["source"],
                    ),
                )
                if i % 100 == 0:
                    print(f"[DB] Inserted {i} rows...")
                    conn.commit()
            conn.commit()
            print(f"[DB] Finished inserting {len(prepared_data)} rows into fuel table")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="download_aircraft_fuel_rates_dag",
    default_args=default_args,
    schedule_interval="@monthly",
    start_date=datetime(2023, 11, 1),
    catchup=True,
    max_active_runs=1,
    tags=["aircraft", "fuel", "etl"],
) as dag:
    fuel_task = PythonOperator(
        task_id="download_and_process_fuel_data",
        python_callable=download_and_process_fuel_rates,
    )
