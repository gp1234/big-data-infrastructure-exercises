import os
from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

import time
import shutil
import requests
from bs4 import BeautifulSoup
import json


settings = Settings()
script_dir = os.path.abspath(os.path.dirname(__file__))
download_dir = os.path.join(settings.raw_dir, "day=20231101")
base_dir = os.path.abspath(os.path.join(script_dir, "..", "..", "data"))
base_url = settings.source_url + "/2023/11/01/"
prepared_dir = os.path.join(base_dir, "concatened")

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)

def check_if_exixts(dir_path):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
    os.makedirs(dir_path, exist_ok=True)

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
    ] = 1,
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
    check_if_exixts(download_dir)
    check_if_exixts(prepared_dir)
    
    response = requests.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")
    
    links = soup.find_all("a", href=True)
    file_links = [link["href"] for link in links if link["href"].endswith(".json.gz")]
    
    file_links = file_links[:file_limit]

    for file_name in file_links:
        file_url = base_url + file_name
        save_path = os.path.join(download_dir, file_name)
        print(f"Downloading {file_url}...")
        file_response = requests.get(file_url, stream=True)
        file_response.raise_for_status()
        with open(save_path, "wb") as file:
            for chunk in file_response.iter_content(chunk_size=8192):
                file.write(chunk)
        time.sleep(2)

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

    
    if os.path.exists(prepared_dir):
        for filename in os.listdir(prepared_dir):
            file_path = os.path.join(prepared_dir, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
    
    for file_name in os.listdir(download_dir):
        if file_name.endswith(".gz"):
            base_name = file_name[:-3] 
            old_file = os.path.join(download_dir, file_name)
            new_file = os.path.join(download_dir, base_name)
            os.rename(old_file, new_file)
            
    for index, file_name in enumerate(os.listdir(download_dir)):
        if file_name.endswith(".json"):  
            file_path = os.path.join(download_dir, file_name)
            output_file_path = os.path.join(prepared_dir, f"{index}.json")
            with open(file_path, "r") as file:
                data = json.load(file)
        if "aircraft" in data:
            aircraft_data = data["aircraft"]
            transformed_aircraft = [
                {
                    "icao": entry.get("hex", ""),
                    "registration": entry.get("r", ""),
                    "type": entry.get("t", ""), 
                }
                for entry in aircraft_data
            ]

        with open(output_file_path, 'w') as f:
            json.dump(transformed_aircraft, f, indent=4)
    
    return "All files renamed successfully!"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    # TODO
    
    file_path = os.path.join(prepared_dir, f"{page}.json")
    if not os.path.exists(file_path):
        return []

    with open(file_path, "r") as file:
        data = json.load(file)
    data.sort(key=lambda x: x.get("icao", ""))

    return data[:num_results]


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    # TODO implement and return a list with dictionaries with those values.
    return [{"timestamp": 1609275898.6, "lat": 30.404617, "lon": -86.476566}]

    
@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    # TODO Gather and return the correct statistics for the requested aircraft
    return {"max_altitude_baro": 300000, "max_ground_speed": 493, "had_emergency": False}
