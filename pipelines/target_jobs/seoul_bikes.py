from utils.config import ConfigLoader
from utils.validator import DFValidator
from transformations.df_transformer import DfTransformer
from utils.metadata import MetadataWriter
from utils.gcp import GcpCredentials

import pandas as pd
from pandas import DataFrame


def parse_config_file(config_path="configs\\target_jobs\seoul_bikes.yml"):
    """
    Return config file contents using given config filepath.
    """
    config = ConfigLoader(config_path).get_config()
    return config


def download_landed_data(config_file: dict) -> DataFrame:
    gcp_bucket = config_file["gcp_bucket"]
    landed_folder = f"{config_file['pipeline_name']}/{config_file['landed_file_name']}"
    landed_file_path = f"gs://{gcp_bucket}/landed/{landed_folder}"
    df = pd.read_csv(
        landed_file_path,
        encoding="unicode_escape",
        storage_options={"token": "libs\keys\service_account.json"},
    )
    return df


def validate_df_exists(df: DataFrame) -> DataFrame:
    """
    Validate that the data has been downloaded by checking row count.
    """
    df_validator = DFValidator()
    return df_validator.validate_df_exists(df)


def apply_transformations(df: DataFrame) -> None:
    """ """
    config = parse_config_file()
    if config["transformations"]:
        for transformation in config["transformations"]:
            for key, value in transformation.items():
                transformer = DfTransformer()
                class_method = getattr(DfTransformer, f"{key}")
                df = class_method(transformer, df, value)
    return df


def add_datetime_metadata(df: DataFrame) -> DataFrame:
    """
    Add datetime stamp to CSV data to confirm
    when data was cleansed.
    """
    metadata_writer = MetadataWriter()
    df = metadata_writer.add_datetime_metadata(df)
    return df


def upload_data_to_storage(config_file: dict, df: DataFrame) -> None:
    """
    Upload data into GCP storage account using GCP storage client.
    """
    gcp_storage_client = GcpCredentials().return_storage_client()
    gcp_bucket = config_file["gcp_bucket"]
    gcp_client_bucket = gcp_storage_client.bucket(gcp_bucket)
    storage_blob = gcp_client_bucket.blob(
        f"cleansed/{config_file['pipeline_name']}/{config_file['cleansed_file_name']}"
    )
    storage_blob.upload_from_string(df.to_csv(index=False), "text/csv")
    return None


def cleanse_seoul_bikes_data():
    """
    Called at file execution - run all neccesary steps of job sequentially.
    """
    config = parse_config_file()
    df = download_landed_data(config)
    df = validate_df_exists(df)
    df = add_datetime_metadata(df)
    df = apply_transformations(df)
    upload_data_to_storage(config, df)


if __name__ == "__main__":
    cleanse_seoul_bikes_data()
