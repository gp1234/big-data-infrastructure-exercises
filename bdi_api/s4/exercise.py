import json
import os
import shutil
import time
from io import BytesIO
from typing import Annotated, Dict, List

import boto3
import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()
s3_client = boto3.client("s3")


def check_if_exists(dir_path):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
    os.makedirs(dir_path, exist_ok=True)


RAW_DOWNLOAD_HISTORY = os.path.join(settings.raw_dir, "day=20231101")

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


@s4.post("/aircraft/download")
def download_data(
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
    ] = 2,
) -> str:
    """Same as s1 but store to an aws s3 bucket taken from settings
    and inside the path `raw/day=20231101/`"""

    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "data/raw/day=20231101/"
    print("Hitting endpoint")
    try:
        try:
            objects = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix_path)
            if "Contents" in objects:
                delete_keys = {"Objects": [{"Key": obj["Key"]} for obj in objects["Contents"]]}
                s3_client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)
        except Exception as e:
            return f"Failed to clean old files on S3 {e}"

        response = requests.get(base_url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")
        links = soup.find_all("a", href=True)
        file_links = [link["href"] for link in links if link["href"].endswith(".json.gz")]
        file_links = file_links[:file_limit]

        successful_uploads: List[str] = []
        failed_uploads: List[Dict[str, str]] = []

        for file_name in file_links:
            file_url = base_url + file_name
            try:
                file_response = requests.get(file_url, stream=True)
                file_response.raise_for_status()
                s3_key = f"{s3_prefix_path}{file_name}"

                file_content = BytesIO(file_response.content)

                s3_client.upload_fileobj(file_content, s3_bucket, s3_key)
                successful_uploads.append(file_name)

            except Exception as e:
                failed_uploads.append({"file": file_name, "error": str(e)})
            time.sleep(1)

    except requests.RequestException:
        return "Failed to fetch data from source"
    except Exception:
        return "Internal server error"

    return f"Uploaded {len(file_links)} files"


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s2.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """
    # TODO
    check_if_exists(settings.prepared_dir)
    check_if_exists(RAW_DOWNLOAD_HISTORY)
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
        print(file_name)
        if file_name.endswith(".json.gz"):
            file_path = os.path.join(RAW_DOWNLOAD_HISTORY, file_name)
            with open(file_path) as file:
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
