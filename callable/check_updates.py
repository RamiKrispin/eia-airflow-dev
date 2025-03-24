import etl.settings as es
import etl.eia_etl as ee
import etl.eia_api as ea
from datetime import datetime
import pointblank as pb

class CheckUpdates:
    def __init__(self):
        pass
    
    def check_updates(path, var):
        settings = es.Settings()
        settings.load_settings(path = path)
        settings.load_api_key(var = var)
        meta = ee.get_metadata(api_key = settings.api_key,
                            api_path = settings.api_meta_path, 
                            meta_path= settings.log_path, facets = 
                            settings.facets, 
                            offset = settings.offset, 
                            window = settings.window)
        
        output = {
            "api_key": settings.api_key,
            "updates_available": str(meta.updates_available),
            "api_path": settings.api_data_path,
            "facets": settings.facets,
            "start": str(meta.start),
            "end": str(meta.api_end_offset)
        }

        return output
    



class DataRefresh:
    def __init__(self):
        pass    
    def refresh_data(self,parameters):
        date_format = "%Y-%m-%d %H:%M:%S"
        start = datetime.strptime(parameters["start"], date_format)
        end = datetime.strptime(parameters["end"], date_format)
        data = ea.eia_get(api_key= parameters["api_key"], 
                            api_path=parameters["api_path"], 
                            data = "value", 
                            facets = parameters["facets"], 
                            start = start, 
                            end = end)
        
        print(data.data.head())
        self.data = data.data
        self.parameters = data.parameters
    def validation(self):
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




def data_refresh(status):
    if status["updates_available"] == "True":
        data = DataRefresh.refresh_data(path = status["path"], var = status["var"])
        return data
    else:
        return "No updates available."

class HelloWorld:
    def __init__(self):
        pass
    
    def say_hello(**kwargs):
        print("Hello, world!")
        print(kwargs.get("path"))
