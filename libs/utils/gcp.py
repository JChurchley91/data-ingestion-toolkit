from google.cloud import storage


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
