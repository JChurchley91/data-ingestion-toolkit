from pandas import DataFrame


class DFValidator:
    def __init__(self):
        pass

    @staticmethod
    def validate_df_exists(df: DataFrame) -> None:
        """
        Return true if length of given dataframe
        is higher than zero rows, return false if not.
        """
        if len(df) > 0:
            return df
        else:
            pass
