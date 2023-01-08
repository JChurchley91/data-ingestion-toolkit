import pytest
from utils.gcp import GcpCredentials
from google.cloud import storage
from google.cloud import bigquery


@pytest.fixture
def return_test_config_path() -> str:
    config_path = "libs/testing/fixtures/test.yml"
    return config_path


@pytest.fixture
def gcp_credentials_object():
    gcp = GcpCredentials()
    return gcp


@pytest.fixture
def return_storage_client() -> storage.Client:
    gcp = GcpCredentials()
    storage_client = gcp.return_storage_client()
    return storage_client


@pytest.fixture
def return_bigquery_client() -> bigquery.Client:
    gcp = GcpCredentials()
    bigquery_client = gcp.return_bigquery_client()
    return bigquery_client


@pytest.fixture
def return_write_config() -> None:
    job_config = bigquery.job.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    return job_config


@pytest.fixture
def return_append_config() -> None:
    job_config = bigquery.job.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    return job_config
