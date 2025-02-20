from fastapi.testclient import TestClient
import os

from bdi_api.settings import Settings

settings = Settings()


FILE_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
WEBSITE_URL = settings.source_url + "/2023/11/01/"
RAW_DOWNLOAD_HISTORY = os.path.join(settings.raw_dir, "day=20231101")
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, "..", "..", "data"))
PREPARED_DIR = os.path.join(BASE_DIRECTORY, "concatened")
PREPARED_FILE_NAME = "concated"

class TestS1Student:
    """
    As you code it's always important to ensure that your code reflects
    the business requisites you have.
    The optimal way to do so is via tests.
    Use this class to create functions to test your application.

    For more information on the library used, search `pytest` in your preferred search engine.
    """
    def test_first(self, client: TestClient) -> None:
        # Implement tests if you want
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=1")
            assert True
            
    def test_download_multiple_files(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=5")
            assert not response.is_error, "Error while downloading multiple files"

    def test_prepare_creates_output(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/prepare")
            assert not response.is_error, "Error while preparing data"
            
            prepared_file  = os.path.join(settings.prepared_dir, f"{settings.prepared_file_name}.json")
            assert os.path.exists(prepared_file), "Prepared file does not exist after processing"

    def test_aircraft_pagination(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s1/aircraft?num_results=2&page=1")
            assert not response.is_error, "Error while testing pagination"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) <= 2, "Pagination is not working correctly"

    def test_positions_empty_result(self, client: TestClient) -> None:
        icao = "0000002821"  
        with client as client:
            response = client.get(f"/api/s1/aircraft/{icao}/positions")
            assert not response.is_error, "Error while fetching positions for non-existent aircraft"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) == 0, "Non-existent aircraft should return an empty list"

    def test_stats_field_types(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{icao}/stats")
            assert not response.is_error, "Error while fetching stats"
            r = response.json()
            assert isinstance(r["max_altitude_baro"], (int, float)), "max_altitude_baro is not a number"
            assert isinstance(r["max_ground_speed"], (int, float)), "max_ground_speed is not a number"
            assert isinstance(r["had_emergency"], bool), "had_emergency is not a boolean"



class TestItCanBeEvaluated:
    """
    Those tests are just to be sure I can evaluate your exercise.
    Don't modify anything from here!

    Make sure all those tests pass with `poetry run pytest` or it will be a 0!
    """

    def test_download(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=1")
            assert not response.is_error, "Error at the download endpoint"

    def test_prepare(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/prepare")
            assert not response.is_error, "Error at the prepare endpoint"

    def test_aircraft(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s1/aircraft")
            assert not response.is_error, "Error at the aircraft endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["icao", "registration", "type"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_positions(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{icao}/positions")
            assert not response.is_error, "Error at the positions endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["timestamp", "lat", "lon"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_stats(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{icao}/stats")
            assert not response.is_error, "Error at the positions endpoint"
            r = response.json()
            for field in ["max_altitude_baro", "max_ground_speed", "had_emergency"]:
                assert field in r, f"Missing '{field}' field."
