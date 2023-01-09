from utils.logger import JobLogger
import datetime
import pytest


def test_return_logs_schema():
    logger = JobLogger()
    schema = logger.return_logs_schema()
    assert schema.__len__() == 9
    assert schema[0].field_type == "INTEGER"


def test_return_logs_table():
    logger = JobLogger()
    logs_table = logger.return_logs_table()
    assert logs_table.dataset_id == "data_ingestion_logs"
    assert logs_table.project == "665587328192"


def test_build_log_df():
    logger = JobLogger()
    log_df = logger.build_log_df(1, "test_job", "test", "test", datetime.datetime.now())
    assert len(log_df) == 1
    assert log_df["job_id"].dtype == "int64"


pytest.main()
