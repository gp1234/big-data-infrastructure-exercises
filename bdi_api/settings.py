from os.path import dirname, join
import os
import shutil
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
import bdi_api

PROJECT_DIR = dirname(dirname(bdi_api.__file__))


class DBCredentials(BaseSettings):
    """Use env variables prefixed with BDI_DB_"""

    host: str
    port: int = 5432
    username: str
    password: str
    model_config = SettingsConfigDict(env_prefix="bdi_db_")


class Settings(BaseSettings):
    source_url: str = Field(
        default="https://samples.adsbexchange.com/readsb-hist",
        description="Base URL to the website used to download the data.",
    )
    local_dir: str = Field(
        default=join(PROJECT_DIR, "data"),
        description="For any other value set env variable 'BDI_LOCAL_DIR'",
    )
    s3_bucket: str = Field(
        default= os.getenv("BDI_S3_BUCKET", "Default"),
        description="Call the api like `BDI_S3_BUCKET=yourbucket poetry run uvicorn...`",
    )
    prepared_file_name: str = Field(
        default="concated",
        description="File name of the processed data",
    )



    telemetry: bool = False
    telemetry_dsn: str = "http://project2_secret_token@uptrace:14317/2"

    model_config = SettingsConfigDict(env_prefix="bdi_")

    @property
    def raw_dir(self) -> str:
        """Store inside all the raw jsons"""
        return join(self.local_dir, "raw")

    @property
    def prepared_dir(self) -> str:
        return join(self.local_dir, "prepared")
    
    @property
    def raw_dir_1(self) -> str:
        """Store inside all the raw jsons"""
        return join(self.aws_dir, "raw")

    @property
    def ensure_directory(self):
        """Ensures a directory exists, recreating it if necessary"""
        def _ensure_dir(dir_path: str) -> str:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
            os.makedirs(dir_path, exist_ok=True)
            return dir_path
        return _ensure_dir

