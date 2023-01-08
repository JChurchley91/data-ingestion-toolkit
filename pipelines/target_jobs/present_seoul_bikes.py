from utils.config import ConfigLoader
from utils.validator import DFValidator
from utils.gcp import GcpCredentials

import pandas as pd
from pandas import DataFrame


def parse_config_file(config_path="configs\\target_jobs\present_seoul_bikes.yml"):
    """
    Return config file contents using given config filepath.
    """
    config = ConfigLoader(config_path).get_config()
    return config


def download_cleansed_data(config_file: dict) -> DataFrame:
    gcp_bucket = config_file["gcp_bucket"]
    cleansed_folder = (
        f"{config_file['pipeline_name']}/{config_file['cleansed_file_name']}"
    )
    cleansed_file_path = f"gs://{gcp_bucket}/cleansed/{cleansed_folder}"
    df = pd.read_csv(
        cleansed_file_path,
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


def create_gcp_dataset(config_file: dict) -> None:
    """
    Create a GCP dataset using a given dataset name within the config.
    """
    gcp_bigquery_client = GcpCredentials().return_bigquery_client()
    bigquery_dataset_name = config_file["presented_dataset_name"]

    try:
        dataset = gcp_bigquery_client.dataset(bigquery_dataset_name)
        dataset = gcp_bigquery_client.create_dataset(
            dataset, timeout=30, exists_ok=True
        )
    except Exception as exception:
        print(exception)

    return None


def create_gcp_table(config_file: dict, df: DataFrame) -> None:
    """
    Create a GCP table using dataset name and table name.
    Load a given dataframe into the table using the write config.
    Table will be truncated if already exists.
    """
    gcp_bigquery_client = GcpCredentials().return_bigquery_client()
    bigquery_write_config = GcpCredentials().return_write_config()

    bigquery_dataset_name = config_file["presented_dataset_name"]
    bigquery_table_name = config_file["presented_table_name"]
    table = f"{bigquery_dataset_name}.{bigquery_table_name}"

    try:
        gcp_bigquery_client.load_table_from_dataframe(
            df, table, job_config=bigquery_write_config
        )
    except Exception as exception:
        print(exception)

    return None


def present_seoul_bikes_data():
    """
    Called at file execution - run all neccesary steps of job sequentially.
    """
    config = parse_config_file()
    df = download_cleansed_data(config)
    df = validate_df_exists(df)
    create_gcp_dataset(config)
    create_gcp_table(config, df)


if __name__ == "__main__":
    present_seoul_bikes_data()
