from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query
from bdi_api.settings import Settings
import boto3
import os

settings = Settings()
s3_client = boto3.client('s3')

FILE_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
RAW_DOWNLOAD_HISTORY = os.path.join(settings.raw_dir, "day=20231101")
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, "..", "..", "data"))
PREPARED_DIR = os.path.join(settings.prepared_dir, "concatened")

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
