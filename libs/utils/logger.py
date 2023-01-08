from google.cloud import bigquery
from utils.gcp import GcpCredentials
from pandas import DataFrame
from datetime import datetime
import pandas as pd


class JobLogger:
    def __init__(self):
        self.bigquery_append_config = GcpCredentials().return_append_config()
        self.bigquery_client = GcpCredentials().return_bigquery_client()
        self.logging_dataset = self.bigquery_client.dataset("data_ingestion_logs")
        self.bigquery_client.create_dataset(self.logging_dataset, exists_ok=True)
        self.logging_table = self.return_logs_table()
        self.job_start_time = datetime.now()
        self.job_start_date = datetime.date(self.job_start_time)

    def return_logs_schema(self) -> list:
        """
        Create the schema needed to create the ingestion job logs schema.
        Defines column names and column data types.
        """
        schema = [
            bigquery.SchemaField("job_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("job_run_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("job_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("job_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("job_status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("job_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("start_time", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("end_time", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("job_duration_seconds", "FLOAT64", mode="REQUIRED"),
        ]
        return schema

    def return_logs_table(self) -> None:
        """
        Return the schema needed to create the ingestion jog logs schema.
        Create and return the ingestion job logs table.
        Will not be recreated if already exists.
        """
        schema = self.return_logs_schema()
        table = bigquery.Table(
            f"665587328192.data_ingestion_logs.job_logs", schema=schema
        )
        return table

    def build_log_df(
        self, job_id, job_name, job_type, job_status, end_time
    ) -> DataFrame:
        """
        Build a dataframe containing the required data fields
        to insert into the job_logs table within bigquery.
        """
        job_run_id = "-".join(
            [str(job_id), str(self.job_start_time.strftime("%Y%m%d%H%M%S"))]
        )
        job_duration_diff = end_time - self.job_start_time
        job_duration = round(job_duration_diff.total_seconds(), 2)
        df = {
            "job_id": job_id,
            "job_run_id": f"{job_run_id}",
            "job_name": f"{job_name}",
            "job_type": f"{job_type}",
            "job_status": job_status,
            "job_date": self.job_start_date,
            "start_time": self.job_start_time,
            "end_time": end_time,
            "job_duration_seconds": job_duration,
        }
        df = pd.DataFrame(data=df, index=[0])
        return df

    def insert_log_df(self, df) -> None:
        """
        Insert a dataframe of any given job log into bigquery.
        Location: data-ingestion-358415.data_ingestion_logs.job_logs.
        """
        try:
            self.bigquery_client.load_table_from_dataframe(
                df, self.logging_table, job_config=self.bigquery_append_config
            )
        except Exception as exception:
            print(exception)

        return None

    def log_job_run(self) -> None:
        pass
