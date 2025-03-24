import os
import json


class Settings:
    def __init__(self):
        pass

    def load_settings(self, path):
        """
        Load settings from a JSON file.

        Args:
            path (str): The path to the JSON file containing the settings.

        Raises:
            FileNotFoundError: If the file at the given path does not exist.
            json.JSONDecodeError: If the file is not a valid JSON.
            KeyError: If the expected keys are not found in the JSON file.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"The file at path {path} does not exist.")

        with open(path, 'r') as raw:
            settings = json.load(raw)

        try:
            self.api_data_path = settings["api"]["api_data_path"]
            self.api_meta_path = settings["api"]["api_meta_path"]
            self.facets = settings["api"]["facets"]
            self.data_path = settings["data"]["data_path"]
            self.log_path = settings["data"]["log_path"]
            self.offset = settings["data"]["offset"]
            self.window = settings["data"]["window"]
        except KeyError as e:
            raise KeyError(f"Missing expected key in settings: {e}")

        self.settings = settings

    def load_api_key(self, var):
        """
        Load an API key from an environment variable.

        Args:
            var (str): The name of the environment variable containing the API key.

        Raises:
            KeyError: If the environment variable is not found.
        """
        try:
            self.api_key = os.environ[var]
        except KeyError:
            raise KeyError(f"Environment variable {var} not found.")