from typing import Optional

import psycopg2
from fastapi import APIRouter, status
from pydantic import BaseModel

from bdi_api.settings import DBCredentials, Settings

settings = Settings()
db_credentials = DBCredentials()

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)

class AircraftReturn(BaseModel):
    icao: str
    registration: Optional[str]
    type: Optional[str]
    owner: Optional[str]
    manufacturer: Optional[str]
    model: Optional[str]

@s8.get("/aircraft/", response_model=list[AircraftReturn])
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    conn = psycopg2.connect(
        host=db_credentials.host,
        port=db_credentials.port,
        dbname=db_credentials.db,
        user=db_credentials.user,
        password=db_credentials.password,
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT t.icao, t.registration, t.type, r.ownop, r.manufacturer, r.model
        FROM traces t
        JOIN aircraft_registry r ON t.icao = r.icao
        ORDER BY t.icao ASC
        LIMIT %s OFFSET %s;
    """, (num_results, page * num_results))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        AircraftReturn(
            icao=row[0],
            registration=row[1],
            type=row[2],
            owner=row[3],
            manufacturer=row[4],
            model=row[5],
        ) for row in rows
    ]

class AircraftCO2(BaseModel):
    icao: str
    hours_flown: float
    co2: Optional[float]

@s8.get("/aircraft/{icao}/co2", response_model=AircraftCO2)
def get_aircraft_co2(icao: str, day: str) -> AircraftCO2:
    conn = psycopg2.connect(
        host=db_credentials.host,
        port=db_credentials.port,
        dbname=db_credentials.db,
        user=db_credentials.user,
        password=db_credentials.password,
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*) FROM traces
        WHERE icao = %s AND to_char(to_timestamp(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS'), 'YYYY-MM-DD') = %s;
    """, (icao, day))
    count = cur.fetchone()[0]

    hours_flown = (count * 5) / 3600.0

    cur.execute("""
        SELECT r.short_type, f.galph FROM aircraft_registry r
        JOIN aircraft_fuel_consumption f ON r.short_type = f.aircraft_type
        WHERE r.icao = %s;
    """, (icao,))

    fuel_info = cur.fetchone()
    cur.close()
    conn.close()

    if fuel_info is None:
        return AircraftCO2(icao=icao, hours_flown=hours_flown, co2=None)

    galph = fuel_info[1]
    fuel_used_gal = hours_flown * galph
    fuel_used_kg = fuel_used_gal * 3.04
    co2_tons = (fuel_used_kg * 3.15) / 907.185

    return AircraftCO2(icao=icao, hours_flown=hours_flown, co2=co2_tons)
