from utils.metadata import MetadataWriter
import pandas as pd
import pytest


def test_add_metadata():
    metadata_writer = MetadataWriter()
    d = {"col1": [1, 2], "col2": [3, 4]}
    df = pd.DataFrame(data=d)
    df = metadata_writer.add_datetime_metadata(df)
    assert df["datetime_loaded"].dtype == "datetime64[ns]"


pytest.main()
