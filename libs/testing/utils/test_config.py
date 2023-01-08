from testing.fixtures.utils import return_test_config_path
from utils.config import YamlLoader, ConfigLoader
import pytest


def test_yaml_loader(return_test_config_path) -> dict:
    config_path = return_test_config_path
    yaml = YamlLoader().load_yaml_file(f"{config_path}")
    assert yaml["job_id"] == 1
    assert yaml["job_name"] == "test"


def test_config_loader(return_test_config_path) -> dict:
    config = ConfigLoader(return_test_config_path).get_config()
    assert config["job_id"] == 1
    assert config["job_name"] == "test"


pytest.main()
