import etl.eia_etl as ee
import etl.eia_api as ea
from datetime import datetime
import pointblank as pb
import os
import json

class CheckUpdates:
    """
    A class to check for updates in the metadata using the EIA API.

    This class interacts with the EIA API to fetch metadata and determine
    if updates are available. It also provides information about the API
    paths, facets, and other settings required for data processing.
    """

    def __init__(self):
        """
        Initialize the CheckUpdates class.
        """
        pass
    
    def check_updates(self, path, var):
        """
        Check for updates in the metadata.

        Args:
            path (str): The path to the settings JSON file.
            var (str): The name of the environment variable containing the API key.

        Raises:
            FileNotFoundError: If the settings file does not exist.
            json.JSONDecodeError: If the settings file is not a valid JSON.
            KeyError: If required keys are missing in the settings or environment variable.

        Sets:
            self.status (dict): A dictionary containing metadata update status and settings.
        """
        settings = Settings()
        settings.load_settings(path = path)
        settings.load_api_key(var = var)
        meta = ee.get_metadata(api_key = settings.api_key,
                            api_path = settings.api_meta_path, 
                            meta_path= settings.log_path, facets = 
                            settings.facets, 
                            offset = settings.offset, 
                            window = settings.window)
        
        output = {
            "updates_available": str(meta.updates_available),
            "api_path": settings.api_data_path,
            "facets": settings.facets,
            "data_validation_path": settings.data_validation_path,
            "data_path": settings.data_path,
            "log_path": settings.log_path,
            "start": str(meta.start),
            "end": str(meta.api_end_offset)
        }

        self.status = output

class DataRefresh:
    """
    A class to handle data refresh operations using the EIA API.

    This class fetches data from the EIA API based on specified parameters,
    validates the data against a predefined schema, and provides methods
    for data validation.
    """

    def __init__(self):
        """
        Initialize the DataRefresh class.
        """
        pass    

    def refresh_data(self, var, parameters):
        """
        Fetch data from the EIA API based on the provided parameters.

        Args:
            var (str): The name of the environment variable containing the API key.
            parameters (dict): A dictionary containing the following keys:
                - "start" (str): The start date in the format "%Y-%m-%d %H:%M:%S".
                - "end" (str): The end date in the format "%Y-%m-%d %H:%M:%S".
                - "api_path" (str): The API path for data retrieval.
                - "facets" (dict): The facets to filter the data.

        Raises:
            KeyError: If the environment variable is not found or required parameters are missing.

        Sets:
            self.data: The data retrieved from the API.
            self.parameters: The parameters used for the API request.
        """
        settings = Settings()
        settings.load_api_key(var = var)
        date_format = "%Y-%m-%d %H:%M:%S"
        start = datetime.strptime(parameters["start"], date_format)
        end = datetime.strptime(parameters["end"], date_format)
        data = ea.eia_get(api_key= settings.api_key, 
                            api_path=parameters["api_path"], 
                            data = "value", 
                            facets = parameters["facets"], 
                            start = start, 
                            end = end)

        self.data = data.data
        self.parameters = data.parameters

    def validation(self):
        """
        Validate the fetched data against a predefined schema.

        This method uses the `pointblank` library to validate the data
        and checks if all validation rules pass.

        Raises:
            Exception: If the validation fails.

        Sets:
            self.validation: The validation object containing the results.
        """
        schema_refresh = pb.Schema(
            columns=[
        ("index", "datetime64[ns]"),   
        ("respondent", "object"),
        ("respondent-name", "object"),
        ("type", "object"),
        ("type-name", "object"),
        ("value", "int64"),
        ("value-units", "object")
        ]
        )
        data_validation = ee.Validation(data = self.data,  
            tbl_name= "get request",
            label = "validation",
            parameters=self.parameters,
            initial= False,
            warning=0.10, 
            error=0, 
            critical=0)

        data_validation.add_schema(schema = schema_refresh)
        data_validation.validate()
        self.validation = data_validation
        print(self.validation.validation.all_passed())




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
            self.data_validation_path = settings["data"]["data_validation_path"]
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

