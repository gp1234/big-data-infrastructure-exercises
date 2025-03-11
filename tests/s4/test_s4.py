import json
import os

from fastapi.testclient import TestClient

from bdi_api.settings import Settings

settings = Settings()

WEBSITE_URL = settings.source_url + "/2023/11/01/"
RAW_DOWNLOAD_HISTORY = os.path.join(settings.raw_dir, "day=20231101")
PREPARED_DIR = os.path.join(settings.prepared_dir)

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

    def test_prepare_creates_output(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s4/aircraft/prepare")
            assert not response.is_error, "Error while preparing data in s4"

            prepared_file = os.path.join(settings.prepared_dir, f"{settings.prepared_file_name}.json")
            assert os.path.exists(prepared_file), "Prepared file does not exist after processing"


    def test_download_and_verify_s3(self, client: TestClient) -> None:
        """Test download and verify files exist in S3"""
        with client as client:
            response = client.post("/api/s4/aircraft/download?file_limit=2")
            assert not response.is_error

    def test_prepare_and_verify_content(self, client: TestClient) -> None:
        """Test prepare endpoint and verify JSON content"""
        with client as client:
            # First ensure we have files in S3
            client.post("/api/s4/aircraft/download?file_limit=2")

            # Then prepare the data
            response = client.post("/api/s4/aircraft/prepare")
            assert not response.is_error

            # Verify the prepared file exists and has valid content
            prepared_file = os.path.join(settings.prepared_dir, f"{settings.prepared_file_name}.json")
            assert os.path.exists(prepared_file)

            with open(prepared_file) as f:
                data = json.load(f)
                assert isinstance(data, list)
                if len(data) > 0:
                    first_item = data[0]
                    assert "icao" in first_item, "Missing required field: icao"
                    assert "traces" in first_item, "Missing required field: traces"
                    if first_item["traces"]:
                        first_trace = first_item["traces"][0]
                        trace_fields = ["registration", "type"]
                        for field in trace_fields:
                            assert field in first_trace, f"Missing required field in trace: {field}"


    def test_full_workflow(self, client: TestClient) -> None:
        """Test the complete workflow: download -> prepare -> verify"""
        with client as client:
            # Download files
            download_response = client.post("/api/s4/aircraft/download?file_limit=3")
            assert not download_response.is_error

            # Prepare files
            prepare_response = client.post("/api/s4/aircraft/prepare")
            assert not prepare_response.is_error

            # Verify prepared file
            prepared_file = os.path.join(settings.prepared_dir, f"{settings.prepared_file_name}.json")
            assert os.path.exists(prepared_file)
            with open(prepared_file) as f:
                data = json.load(f)
                assert isinstance(data, list)
                assert len(data) > 0
