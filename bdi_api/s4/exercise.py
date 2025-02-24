import json
import os
import shutil
import time
from io import BytesIO
from typing import Annotated, Dict, List

import asyncio
import aiohttp
import boto3
from bs4 import BeautifulSoup
from fastapi import APIRouter, status, Query

from bdi_api.settings import Settings

settings = Settings()
s3_client = boto3.client("s3")

RAW_DOWNLOAD_HISTORY = os.path.join(settings.raw_dir, "day=20231101")

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)

async def download_file(session, file_url, local_path):
    try:
        async with session.get(file_url) as response:
            response.raise_for_status()
            with open(local_path, 'wb') as f:
                f.write(await response.read())
            return True, None
    except Exception as e:
        return False, {"file": file_url, "error": str(e)}

def upload_file_to_s3(local_path, s3_bucket, s3_key):
    try:
        with open(local_path, 'rb') as f:
            s3_client.upload_fileobj(f, s3_bucket, s3_key)
        return True, None
    except Exception as e:
        return False, {"file": local_path, "error": str(e)}

@s4.post("/aircraft/download")
async def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Same as s1 but store to an aws s3 bucket taken from settings
    and inside the path `raw/day=20231101/`"""

    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "data/raw/day=20231101/"

    try:

        objects = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix_path)
        if "Contents" in objects:
            delete_keys = {"Objects": [{"Key": obj["Key"]} for obj in objects["Contents"]]}
            s3_client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)

        async with aiohttp.ClientSession() as session:
            response = await session.get(base_url)
            response.raise_for_status()
            soup = BeautifulSoup(await response.text(), "lxml")
            links = soup.find_all("a", href=True)
            file_links = [link["href"] for link in links if link["href"].endswith(".json.gz")]
            file_links = file_links[:file_limit]

            tasks = []
            for file_name in file_links:
                file_url = base_url + file_name
                local_path = os.path.join(RAW_DOWNLOAD_HISTORY, file_name)
                tasks.append(download_file(session, file_url, local_path))

            results = await asyncio.gather(*tasks)

            successful_downloads = [file_links[i] for i, (success, _) in enumerate(results) if success]
            failed_downloads = [result[1] for result in results if result[1]]

            # Upload files to S3
            upload_results = []
            for file_name in successful_downloads:
                local_path = os.path.join(RAW_DOWNLOAD_HISTORY, file_name)
                s3_key = f"{s3_prefix_path}{file_name}"
                upload_results.append(upload_file_to_s3(local_path, s3_bucket, s3_key))

    except aiohttp.ClientError:
        return "Failed to fetch data from source"
    except Exception as e:
        return f"Internal server error: {e}"

    return f"Downloaded {len(successful_downloads)} files. "


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s2.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """
    settings.ensure_directory(settings.prepared_dir)
    settings.ensure_directory(RAW_DOWNLOAD_HISTORY)
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "data/raw/day=20231101/"
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix_path)
        if "Contents" not in response:
            return "No data found in S3 bucket prefix"
    except Exception:
        return "No data found in S3 bucket prefix"
    
    for obj in response["Contents"]:
        file_key = obj["Key"]
        file_name = os.path.basename(file_key)
        local_file_path = os.path.join(RAW_DOWNLOAD_HISTORY, file_name)

        try:
            s3_client.download_file(s3_bucket, file_key, local_file_path)
        except Exception as e:
            print(f"Error downloading {file_key}: {str(e)}")

    output_file_path = os.path.join(settings.prepared_dir, f"{settings.prepared_file_name}.json")

    all_transformed_aircraft = []
    for _index, file_name in enumerate(os.listdir(RAW_DOWNLOAD_HISTORY)):
        if file_name.endswith(".json.gz"):
            file_path = os.path.join(RAW_DOWNLOAD_HISTORY, file_name)
            with open(file_path, 'rt') as file:
                data = json.load(file)
            if "aircraft" in data:
                aircraft_data = data["aircraft"]

                transformed_aircraft = [
                    {
                        "icao": entry.get("hex", ""),
                        "registration": entry.get("r", ""),
                        "type": entry.get("t", ""),
                        "lat": entry.get("lat", ""),
                        "lon": entry.get("lon", ""),
                        "timestamp": entry.get("seen_pos", ""),
                        "max_alt_baro": entry.get("max_alt", ""),
                        "max_ground_speed": entry.get("gs", ""),
                        "had_emergency": (
                            entry.get("emergency", "").lower() != "none" if entry.get("emergency") else False
                        ),
                        "file_name": file_name,
                    }
                    for entry in aircraft_data
                ]

                all_transformed_aircraft.extend(transformed_aircraft)

    grouped_aircraft = {}
    for aircraft in all_transformed_aircraft:
        icao = aircraft["icao"]
        if icao not in grouped_aircraft:
            grouped_aircraft[icao] = []
        grouped_aircraft[icao].append(aircraft)

    all_transformed_aircraft = [
        {"icao": key, "traces": [{k: v for k, v in trace.items() if k != "icao"} for trace in value]}
        for key, value in grouped_aircraft.items()
    ]
    with open(output_file_path, "w") as f:
        json.dump(all_transformed_aircraft, f, indent=4)

    return "Files have been prepared"
