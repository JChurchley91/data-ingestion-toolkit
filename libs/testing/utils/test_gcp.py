from testing.fixtures.utils import (
    gcp_credentials_object,
)
from utils.gcp import GcpCredentials
import pytest


def test_init(gcp_credentials_object):
    gcp = gcp_credentials_object
    assert gcp.project_id == "665587328192"


def test_return_storage_client(gcp_credentials_object):
    storage_client = gcp_credentials_object.return_storage_client()
    assert storage_client.project == "data-ingestion-toolkit"


def test_return_bigquery_client(gcp_credentials_object):
    bigquery_client = gcp_credentials_object.return_bigquery_client()
    assert bigquery_client.get_dataset("seoul_bikes").dataset_id == "seoul_bikes"


def test_return_write_config(gcp_credentials_object):
    gcp = gcp_credentials_object
    write_config = gcp.return_write_config()
    assert (
        write_config.__dict__["_properties"]["load"]["writeDisposition"]
        == "WRITE_TRUNCATE"
    )


def test_return_append_config(gcp_credentials_object):
    gcp = gcp_credentials_object
    append_config = gcp.return_append_config()
    assert (
        append_config.__dict__["_properties"]["load"]["writeDisposition"]
        == "WRITE_APPEND"
    )


pytest.main()
