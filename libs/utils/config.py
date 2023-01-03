import yaml

class YamlLoader:
    def __init__(self):
        pass

    @staticmethod
    def load_yaml_file(file_name: str) -> list:
        """
        Load a given YAML file into a list.
        Return the contents of the YAML file.
        """
        with open(f"{file_name}", "r") as stream:
            try:
                yaml_file_contents = yaml.safe_load(stream)
                return yaml_file_contents
            except yaml.YAMLError as exc:
                print(exc)


class ConfigLoader:
    def __init__(self, config_filepath: str):
        self._config_filepath = config_filepath
        self._yaml_loader = YamlLoader()

    def get_config(self) -> list:
        """
        Instantiate a YAML loader object
        using a given YAML config filepath.
        Return the YAML config as a list.
        """
        config = self._yaml_loader.load_yaml_file(self._config_filepath)
        return config

