from pandas import DataFrame
from datetime import datetime


class MetadataWriter:
    def __init__(self):
        self.pipeline_run_date = datetime.now()

    def add_datetime_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add a column to a given dataframe
        containing the current datetime.
        Return the df.
        """
        df["datetime_loaded"] = self.pipeline_run_date
        return df
