from google.cloud import storage
from google.cloud import bigquery


class GcpCredentials:
    def __init__(self):
        self._project_id = "665587328192"

    @staticmethod
    def return_storage_client() -> storage.Client:
        """
        Return a GCP storage account client.
        Validated with service account credentials
        stored within JSON file.
        """
        client = storage.Client.from_service_account_json(
            json_credentials_path="libs\keys\service_account.json"
        )
        return client

    @staticmethod
    def return_bigquery_client() -> bigquery.Client:
        """
        Return a bigquery client object to interact with bigquery.
        """
        client = bigquery.Client(project="665587328192")
        return client

    @staticmethod
    def return_write_config() -> None:
        """
        Return the config needed to write to any given bigquery table.
        Tables with this config are truncated and created if needed.
        """
        job_config = bigquery.job.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
        return job_config
