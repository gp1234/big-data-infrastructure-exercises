from typing import Annotated
import shutil
from fastapi import APIRouter, status
from fastapi.params import Query
from bdi_api.settings import Settings
import boto3
import os
import requests
import time
from bs4 import BeautifulSoup

settings = Settings()
s3_client = boto3.client('s3')

def check_if_exists(dir_path):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
    os.makedirs(dir_path, exist_ok=True)


RAW_DOWNLOAD_HISTORY = os.path.join(settings.raw_dir_1, "day=20231101")

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
    ] = 100,
) -> str:
    """Same as s1 but store to an aws s3 bucket taken from settings
    and inside the path `raw/day=20231101/`

    NOTE: you can change that value via the environment variable `BDI_S3_BUCKET`
    """
    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"
    # TODO
    check_if_exists(RAW_DOWNLOAD_HISTORY)
    response = requests.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")

    links = soup.find_all("a", href=True)
    file_links = [link["href"] for link in links if link["href"].endswith(".json.gz")]

    file_links = file_links[:file_limit]

    for file_name in file_links:
        file_url = base_url + file_name
        save_path = os.path.join(RAW_DOWNLOAD_HISTORY, file_name)
        print(f"Downloading {file_url}...")
        file_response = requests.get(file_url, stream=True)
        file_response.raise_for_status()
        with open(save_path, "wb") as file:
            for chunk in file_response.iter_content(chunk_size=8192):
                file.write(chunk)
        time.sleep(1)

    return f"Downloaded {len(file_links)} files" 
    """
            s3_client.upload_file(
            os.path.abspath(os.path.join('./bdi_api/s4/', 'example.txt')),
            s3_bucket,
            "test/example2.txt"
        )
    
    
    """
    try:
        print(settings)
        return "File uploaded successfully"
    except Exception as e:
        return f"Error uploading file: {str(e)}"


    return "OK"


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s2.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """
    # TODO
    return "OK"
