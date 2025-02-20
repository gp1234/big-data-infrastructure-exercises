import json
import os
import shutil
import time
from typing import Annotated

import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

WEBSITE_URL = settings.source_url + "/2023/11/01/"
RAW_DOWNLOAD_HISTORY = os.path.join(settings.raw_dir, "day=20231101")


s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)


@s1.post("/aircraft/download")
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
    ] = 100,
) -> str:
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """

    # TODO Implement download

    settings.ensure_directory(RAW_DOWNLOAD_HISTORY)
    settings.ensure_directory(settings.prepared_dir)

    response = requests.get(WEBSITE_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")

    links = soup.find_all("a", href=True)
    file_links = [link["href"] for link in links if link["href"].endswith(".json.gz")]

    file_links = file_links[:file_limit]

    for file_name in file_links:
        file_url = WEBSITE_URL + file_name
        save_path = os.path.join(RAW_DOWNLOAD_HISTORY, file_name)

        file_response = requests.get(file_url, stream=True)
        file_response.raise_for_status()
        with open(save_path, "wb") as file:
            for chunk in file_response.iter_content(chunk_size=8192):
                file.write(chunk)
        time.sleep(1)

    return f"Downloaded {len(file_links)} files"


@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepare the data in the way you think it's better for the analysis.

    * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    * documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.

    TIP: always clean the prepared folder before writing again to avoid having old files.

    Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
    """
    # TODO
    output_file_path = os.path.join(settings.prepared_dir, f"{settings.prepared_file_name}.json")
    if not os.path.exists(RAW_DOWNLOAD_HISTORY) or len(os.listdir(RAW_DOWNLOAD_HISTORY)) == 0:
        return "There are no files to be processsed"

    if os.path.exists(settings.prepared_dir):
        for filename in os.listdir(settings.prepared_dir):
            file_path = os.path.join(settings.prepared_dir, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

    for file_name in os.listdir(RAW_DOWNLOAD_HISTORY):
        if file_name.endswith(".gz"):
            base_name = file_name[:-3]
            old_file = os.path.join(RAW_DOWNLOAD_HISTORY, file_name)
            new_file = os.path.join(RAW_DOWNLOAD_HISTORY, base_name)
            os.rename(old_file, new_file)

    all_transformed_aircraft = []
    for _index, file_name in enumerate(os.listdir(RAW_DOWNLOAD_HISTORY)):
        if file_name.endswith(".json"):
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


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    # TODO

    file_path = os.path.join(settings.prepared_dir, f"{settings.prepared_file_name}.json")
    if not os.path.exists(file_path):
        return []

    with open(file_path) as file:
        data = json.load(file)

    start_index = page * num_results
    end_index = start_index + num_results

    if start_index >= len(data):
        return []
    end_index = min(end_index, len(data))

    data = data[start_index:end_index]

    data.sort(key=lambda x: x.get("icao"))

    return [
        {
            "icao": airplaine.get("icao", ""),
            "registration": airplaine.get("traces", [{}])[0].get("registration", None),
            "type": airplaine.get("traces", [{}])[0].get("type", None),
        }
        for airplaine in data
    ]


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    # TODO implement and return a list with dictionaries with those values.
    file_path = os.path.join(settings.prepared_dir, f"{settings.prepared_file_name}.json")
    if not os.path.exists(file_path):
        return []

    with open(file_path) as file:
        data = json.load(file)

    aircraft = next((item for item in data if item["icao"] == icao), None)
    if not aircraft:
        return []

    traces = aircraft.get("traces", [])
    traces.sort(key=lambda x: float(x.get("timestamp")) if x.get("timestamp") else float("inf"))

    start_index = page * num_results
    end_index = start_index + num_results

    if start_index >= len(traces):
        return []
    end_index = min(end_index, len(traces))

    traces = traces[start_index:end_index]

    return [{"lat": pos.get("lat"), "lon": pos.get("lon"), "timestamp": pos.get("timestamp")} for pos in traces]


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    # TODO Gather and return the correct statistics for the requested aircraft
    file_path = os.path.join(settings.prepared_dir, f"{settings.prepared_file_name}.json")
    if not os.path.exists(file_path):
        return []

    with open(file_path) as file:
        data = json.load(file)

    positions = next((item for item in data if item["icao"] == icao), None)
    if not positions:
        return []
    return {
        "max_altitude_baro": max(
            float(pos.get("max_alt_baro")) if pos.get("max_alt_baro") else 0 for pos in positions.get("traces", [])
        ),
        "max_ground_speed": max(
            float(pos.get("max_ground_speed")) if pos.get("max_ground_speed") else 0
            for pos in positions.get("traces", [])
        ),
        "had_emergency": any(pos.get("had_emergency", False) for pos in positions.get("traces", [])),
    }
