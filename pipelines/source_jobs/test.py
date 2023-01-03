from utils.config import ConfigLoader


def get_config_file(config_path="configs\source_jobs\seoul_bikes.yml"):
    config = ConfigLoader(config_path).get_config()
    return config


config = get_config_file()
print(config)
