import os
import json
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

    def test_download_one_file_s4(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s4/aircraft/download?file_limit=1")
            assert not response.is_error, "Error at the s4 download endpoint"

    def test_download_multiple_files_s4(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s4/aircraft/download?file_limit=5")
            assert not response.is_error, "Error while downloading multiple files in s4"

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
            assert "uploaded successfully" in response.text.lower()

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
                    assert "icao" in first_item
                    assert "registration" in first_item
                    assert "type" in first_item

    def test_prepare_with_empty_s3(self, client: TestClient) -> None:
        """Test prepare endpoint when S3 is empty"""
        with client as client:
            response = client.post("/api/s4/aircraft/prepare")
            assert "No data found in S3 bucket prefix" in response.text

    def test_download_large_batch(self, client: TestClient) -> None:
        """Test downloading a larger batch of files"""
        with client as client:
            response = client.post("/api/s4/aircraft/download?file_limit=10")
            assert not response.is_error
            assert "uploaded successfully" in response.text.lower()

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
