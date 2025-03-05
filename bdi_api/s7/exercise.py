from fastapi import APIRouter, status
import boto3
from io import BytesIO
import os
import json
import psycopg2
from bdi_api.settings import DBCredentials, Settings

settings = Settings()
##db_credentials = DBCredentials()
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)

def get_connections():
    conn = psycopg2.connect(
    dbname="bts_infra",
    user="bts",
    password="bts",
    host="localhost",
    port="5432"
    )
    cur = conn.cursor()
    return conn, cur

def ensure_tables_exist(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS traces (
    id SERIAL PRIMARY KEY,
    icao TEXT NOT NULL,
    registration TEXT NULL,
    type TEXT NULL,
    lat TEXT,
    lon TEXT,
    timestamp TEXT,
    max_alt_baro TEXT,
    max_ground_speed TEXT,
    had_emergency BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_traces_icao ON traces (icao);
  
    """)
    
def process_file(s3_bucket_name, file_key, s3_client, cursor):
    response = s3_client.get_object(Bucket=s3_bucket_name, Key=file_key)
    data = json.load(BytesIO(response["Body"].read()))

    for entry in data.get("aircraft", []):
        icao = entry.get("hex")

        cursor.execute("""
            INSERT INTO traces (icao, lat, lon, timestamp, max_alt_baro, max_ground_speed, had_emergency, registration, type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            icao, 
            entry.get("lat"), 
            entry.get("lon"), 
            entry.get("seen_pos"), 
            entry.get("alt_baro"), 
            entry.get("gs"), 
            entry.get("alert") == 1,
            entry.get("r", None),  # Fixed column name
            entry.get("t", None)   # Fixed column name
        ))

@s7.post("/aircraft/prepare")
def prepare_data() -> str:
    """Get the raw data from s3 and insert it into RDS

    Use credentials passed from `db_credentials`
    """
    ##user = db_credentials.username
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "data/raw/day=20231101/"
    # TODO
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix_path)

    conn, cur = get_connections()

    ensure_tables_exist(cur)

    for page in page_iterator:
        for obj in page.get("Contents", []):
            process_file(s3_bucket, obj["Key"], s3_client, cur)

    conn.commit()
    cur.close()
    conn.close()
    return "OK"


@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    # TODO
    conn, cur = get_connections()
    offset = page * num_results
    cur.execute("""
        SELECT DISTINCT ON (icao) icao, registration, type
        FROM traces
        ORDER BY icao ASC
        LIMIT %s OFFSET %s;
    """, (num_results, offset))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if rows:
     return [{"icao": row[0], "registration": row[1], "type": row[2]} for row in rows]
    else:    
        return [{}]
    

@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list. FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    # TODO
    conn, cur = get_connections()
    
    offset = page * num_results
    cur.execute("""
        SELECT timestamp, lat, lon
        FROM traces
        WHERE icao = %s
        ORDER BY timestamp ASC
        LIMIT %s OFFSET %s;
    """, (icao, num_results, offset))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if rows:
        results = [{"timestamp": row[0], "lat": row[1], "lon": row[2]} for row in rows]
        return results
    else:
        return [{}]
    

    
  


@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency

    FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    # TODO
    conn, cur = get_connections()
    cur.execute("""
        SELECT 
            MAX(max_alt_baro) AS max_altitude_baro,
            MAX(max_ground_speed) AS max_ground_speed,
            BOOL_OR(had_emergency) AS had_emergency
        FROM traces
        WHERE icao = %s;
    """, (icao,))

    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        result = {
            "max_altitude_baro": row[0] if row[0] is not None else 0,
            "max_ground_speed": row[1] if row[1] is not None else 0,
            "had_emergency": row[2] if row[2] is not None else False
        }
        return result
    else:
        return {}


    
   