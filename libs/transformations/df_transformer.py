from utils.config import ConfigLoader
from pandas import DataFrame


class DfTransformer:
    def __init__(self) -> None:
        pass

    def create_index_primary_key(self, df: DataFrame, column_name=None) -> DataFrame:
        """
        Create an index primary key column on a given DF.
        Insert the column at position zero of the DF.
        Return the DF.
        """
        df.index = range(1, df.shape[0] + 1)
        df.insert(0, f"{column_name}", df.index)
        return df

    def apply_schema(self, df: DataFrame, schema_filepath: str) -> DataFrame:
        """
        Apply a schema to a given DF as
        defined in a given YAML file.
        Return the DF.
        """
        config = ConfigLoader(schema_filepath).get_config()
        target_schema = config["schema"]
        df.columns = target_schema
        return df
