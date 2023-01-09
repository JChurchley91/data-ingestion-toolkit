from utils.validator import DFValidator
import pandas as pd
import pytest


def test_validate_df_exists():
    df_validator = DFValidator()
    d = {"col1": [1, 2], "col2": [3, 4]}
    df = pd.DataFrame(data=d)
    is_valid_df = df_validator.validate_df_exists(df)
    assert len(is_valid_df) == 2


pytest.main()
