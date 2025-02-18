from fastapi.testclient import TestClient
import os

from bdi_api.settings import Settings

settings = Settings()

FILE_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
WEBSITE_URL = settings.source_url + "/2023/11/01/"
RAW_DOWNLOAD_HISTORY = os.path.join(settings.raw_dir, "day=20231101")
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, "..", "..", "data"))
PREPARED_DIR = os.path.join(BASE_DIRECTORY, "concatened")
PREPARED_FILE_NAME = "aircraft.json"


class TestS4Student:
    """
    As you code it's always important to ensure that your code reflects
    the business requisites you have.
    The optimal way to do so is via tests.
    Use this class to create functions that test your s4 application.

    For more information on the library used, search `pytest` in your preferred search engine.
    """

    def test_download_one_file(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s4/aircraft/download?file_limit=1")
            assert not response.is_error, "Error at the s4 download endpoint"

    def test_download_multiple_files(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s4/aircraft/download?file_limit=5")
            assert not response.is_error, "Error while downloading multiple files in s4 from AWS"

    def test_download_one_file_s1(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=1")
            assert not response.is_error, "Error at the s1 download endpoint"

    def test_download_multiple_files_s1(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=5")
            assert not response.is_error, "Error while downloading multiple files in s1"

    def test_prepare_creates_output(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s4/aircraft/prepare")
            assert not response.is_error, "Error while preparing data in s4"

            prepared_file = os.path.join(settings.prepared_dir, PREPARED_FILE_NAME)
            assert os.path.exists(prepared_file), "Prepared file does not exist after processing"