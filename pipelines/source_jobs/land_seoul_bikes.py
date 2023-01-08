from utils.config import ConfigLoader
from utils.validator import DFValidator
from utils.metadata import MetadataWriter
from utils.gcp import GcpCredentials

import pandas as pd
from pandas import DataFrame


def parse_config_file(config_path="configs\source_jobs\land_seoul_bikes.yml"):
    """
    Return config file contents using given config filepath.
    """
    config = ConfigLoader(config_path).get_config()
    return config


def download_csv_data(config_file: dict) -> DataFrame:
    """
    Download CSV file data from web using given web file path.
    """
    df = pd.read_csv(config_file["web_file_path"], encoding="unicode_escape")
    return df


def validate_df_exists(df: DataFrame) -> DataFrame:
    """
    Validate that the data has been downloaded by checking row count.
    """
    df_validator = DFValidator()
    return df_validator.validate_df_exists(df)


def upload_data_to_storage(config_file: dict, df: DataFrame) -> None:
    """
    Upload data into GCP storage account using GCP storage client.
    """
    gcp_storage_client = GcpCredentials().return_storage_client()
    gcp_bucket = config_file["gcp_bucket"]
    gcp_client_bucket = gcp_storage_client.bucket(gcp_bucket)
    storage_blob = gcp_client_bucket.blob(
        f"landed/{config_file['pipeline_name']}/{config_file['landed_file_name']}"
    )
    storage_blob.upload_from_string(df.to_csv(index=False), "text/csv")
    return None


def ingest_seoul_bikes_data():
    """
    Called at file execution - run all neccesary steps of job sequentially.
    """
    config = parse_config_file()
    df = download_csv_data(config)
    df = validate_df_exists(df)
    upload_data_to_storage(config, df)


if __name__ == "__main__":
    ingest_seoul_bikes_data()
