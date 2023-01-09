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
