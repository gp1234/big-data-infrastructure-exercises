from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings
import boto3
import requests
import tempfile
import os



settings = Settings()

s2 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s2",
    tags=["s2"],
)

s3_client = boto3.client('s3')

@s2.post("/aircraft/download")
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

    s3_bucket_name = settings.s3_bucket_name

    try:
        s3_client.upload_file(
            os.path.abspath(os.path.join('./s2/', 'example.txt')),
            'bdi-aircraft-gio-s3',
            "test/example.txt"
        )
        return "File uploaded successfully"
    except Exception as e:
        return f"Error uploading file: {str(e)}"

    return "Files have been downloaded and uploaded to S3"


@s2.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s2.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """
    # TODO
    return "OK"
