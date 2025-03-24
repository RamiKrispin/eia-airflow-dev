import pandas as pd
import datetime
import etl.eia_api as ea
import pointblank as pb
import os

def check_file_exists(file_path):
    """
    Check if a file exists at the given file path.

    Parameters:
    file_path (str): The path to the file.

    Returns:
    bool: True if the file exists, False otherwise.
    """
    return os.path.isfile(file_path)


class Metadata:
    def __init__(self, metadata, last_index, request_meta):
        self.metadata = metadata
        self.last_index = last_index
        self.request_meta = request_meta

def load_metadata(path, facets):
    """
    Load metadata from a CSV file and prepare request metadata.

    Parameters:
    path (str): The file path to the metadata CSV.
    facets (dict): The series facets list containing respondent and type.

    Returns:
    Metadata: An object containing the loaded metadata, last index, and request metadata.
    """
    meta = pd.read_csv(path)
    meta["time"] = pd.to_datetime(meta["time"])
    meta["start"] = pd.to_datetime(meta["start"])
    meta["start_act"] = pd.to_datetime(meta["start_act"])
    meta["end"] = pd.to_datetime(meta["end"])
    meta["end_act"] = pd.to_datetime(meta["end_act"])

    log = {
        "respondent": None,
        "type": None,
        "end_act": None,
        "request_start": None
    }

    meta_success = meta[meta["status"] == True]
    request_meta = pd.DataFrame()

    r = facets["respondent"]
    t = facets["type"]
    l = meta_success[(meta_success["respondent"] == r) & (meta_success["type"] == t)]
    
    
    
    
    l = l[l["index"] == l["index"].max()]
    log["respondent"] = r
    log["type"] = t
    log["end_act"] = l["end_act"].max()
    log["request_start"] = l["end_act"].max() + datetime.timedelta(hours=1)
    request_meta = request_meta._append(pd.DataFrame([log]), ignore_index=True)

    return Metadata(metadata=meta, last_index=l["index"].max(), request_meta=request_meta)


class ApiStatus:
    def __init__(self, updates_available, local_end, api_end, api_end_offset, offset, window, start):
        self.updates_available=updates_available
        self.local_end=local_end
        self.api_end=api_end
        self.api_end_offset=api_end_offset
        self.offset=offset
        self.window=window
        self.start=start


def get_metadata(api_key, api_path, meta_path, facets, window = None, offset=None):
    """
    Get metadata from the EIA API and update request metadata.

    Parameters:
    api_key (str): The API key for accessing the EIA API.
    api_path (str): The specific API path to access.
    meta_path (str): The file path to the metadata CSV.
    facets (pd.DataFrame): The series facets DataFrame containing respondent and type.
    window (int, optional): The overlap window between the request starting point and the local end point. Defaults to None.
    offset (int, optional): The offset in hours to adjust the end period. Defaults to None.

    Returns:
    ApiStatus: An object contains information about the data available in the API status with respect to the local data
    """
    meta = load_metadata(path=meta_path, facets=facets)
    api_metadata = ea.eia_metadata(api_key=api_key, api_path=api_path)
    api_end = pd.to_datetime(api_metadata.meta["endPeriod"])

    if offset is not None:
        api_end_offset = api_end - datetime.timedelta(hours=offset)
    else:
        api_end_offset = api_end    
        offset = 0
    m = meta.metadata[(meta.metadata["status"] == True) & (meta.metadata["update"] == True)]    
    m = m[m["time"] == m["time"].max()]
    local_end = m["end_act"].max()
    
    if window is not None:
        start = local_end - datetime.timedelta(hours=window)
    else:
        start = local_end
        window = 0 

    if api_end_offset > local_end and start < api_end_offset:
        updates_available = True
    else:
        updates_available = False
        start = None

    print("Last timestamp (local): " + local_end.strftime("%Y-%m-%dT%H"))
    print("Last timestamp (API): " + api_end.strftime("%Y-%m-%dT%H"))
    print("Offset: " + str(offset))
    print("Window: " + str(window))
    print("New data:", str(updates_available))
    if updates_available:
        print("Start time:" + start.strftime("%Y-%m-%dT%H"))
        print("End time:" + api_end.strftime("%Y-%m-%dT%H"))
        print("End time (offset):" + api_end_offset.strftime("%Y-%m-%dT%H"))

    return ApiStatus(updates_available= updates_available, 
                    local_end=local_end, 
                    api_end = api_end, 
                    api_end_offset = api_end_offset,
                    offset= offset,
                    window= window, 
                    start = start)


class Validation:
    def __init__(self, data, tbl_name, label, parameters, initial, warning=0, error=0, critical=0):
        self.data=data
        self.tbl_name=tbl_name
        self.label=label
        self.warning=warning
        self.error=error
        self.critical=critical
        self.parameters=parameters
        self.initial = initial
        
    def add_schema(self, schema):
        self.schema = schema

    def validate(self):
        validation = (
            pb.Validate(data = self.data,
            tbl_name= self.tbl_name,
            label = self.label,
            thresholds=pb.Thresholds(warning=self.warning, error=self.error, critical=self.critical))
            .col_schema_match(schema=self.schema)
            .col_vals_gt(columns="value", value=0)
            .col_count_match(count=len(self.schema.columns)) 
            .col_vals_in_set(columns="respondent", set = [self.parameters["facets"]["respondent"]])
            .col_vals_in_set(columns="type", set = [self.parameters["facets"]["type"]])
            .col_vals_not_null(columns= ["index","value"])
            .rows_distinct() 
            .interrogate()
        )

        self.validation = validation
        self.status = validation.all_passed()
        self.n_obs = len(self.data)



class Log:
    def __init__(self):
        pass
    
    def create_log(self, facets):
        log = {
            "index": None,
            "respondent": facets["respondent"],
            "type": facets["type"],
            "time": datetime.datetime.now(datetime.timezone.utc),
            "start": None,
            "end": None,
            "start_act": None,
            "end_act": None,
            "start_match": None,
            "end_match": None,
            "n_obs": None,
            "validation": None,
            "status": None,
            "refresh_type": None,
            "update": None,
            "comments": None
        }

        self.log = log
    def no_updates(self):
        self.log["status"] = False
        self.log["update"] = False
        self.log["refresh_type"] = "refresh"
        self.log["comments"] = "There is no new data in the API"
    
    def fauilure(self):
        self.log["status"] = False
        self.log["update"] = False
        self.log["refresh_type"] = "refresh"
        self.log["comments"] = "The data validation failed, please check the log"

    def add_validation(self, validation):
        if not isinstance(validation.parameters["start"], datetime.datetime):
            validation.parameters["start"] = pd.to_datetime(validation.parameters["start"])
        if not isinstance(validation.parameters["end"], datetime.datetime):
            validation.parameters["end"] = pd.to_datetime(validation.parameters["end"])
        
        self.log["start"] = validation.parameters["start"]
        self.log["end"] = validation.parameters["end"]
        self.log["start_act"] = validation.data["index"].min()
        self.log["end_act"] = validation.data["index"].max()
        self.log["start_match"] = self.log["start"] == self.log["start_act"]
        self.log["end_match"] = self.log["end"] == self.log["end_act"]
        self.log["validation"] = validation.status
        self.log["status"] = None
        self.log["update"] = None
        self.log["n_obs"] = validation.n_obs
        self.log["comments"] = ""
        if validation.initial:
            self.log["refresh_type"] = "backfill"
        else:
            self.log["refresh_type"] = "refresh"

        if self.log["start_match"] and self.log["validation"]:
            self.log["status"] = True
        else:
            self.log["status"] = False
            self.log["comments"] = self.log["comments"] + "The data checks failed, please check the log; "
        if not self.log["end_match"]:
            self.log["comments"] = self.log["comments"] + "The series last timestamp does not match the end argument, please check the log; "


class AppendData:
    pass

    def append_data(self, data_path, new_data, schema, parameters, save=False):
        """
        Append new data to an existing CSV file.

        Parameters:
        data_path (str): The file path to the existing data CSV.
        new_data (pd.DataFrame): The new data to append.
        schema (pointblank.schema.Schema): The schema to validate the data.
        save (bool, optional): Whether to save the appended data back to the CSV file. Defaults to False.

        Returns:
        None

        Attributes:
        input (pd.DataFrame): The original data read from the CSV file.
        new_data (pd.DataFrame): The new data to append.
        output (pd.DataFrame): The combined data after appending and sorting.
        status (bool): The status of the append operation.
        save (bool): Whether the data was saved back to the CSV file.
        """
        d1 = pd.read_csv(data_path)
        d1["index"] = pd.to_datetime(d1["index"])
        l1 = len(d1)

        d1 = d1[["index", "respondent", "type", "value", "value-units"]]

        d2 = new_data[["index", "respondent", "type", "value", "value-units"]]
        l2 = len(d2)
        d1 = d1[d1["index"] < d2["index"].min()]
        l3 = l1 - len(d1)
        df = pd.concat([d1, d2])
        df = df.sort_values(by="index")
        df.reset_index(drop=True, inplace=True)
        print("Appending " + str(l2) + " observations")
        print("Overlapping of " + str(l3) + " observations")
        # Add validation step
        test = Validation(data = df,  
                    tbl_name= "get request",
                    label = "validation",
                    parameters=parameters,
                    initial= False,
                    warning=0.10, 
                    error=0, 
                    critical=0)
        
        test.add_schema(schema = schema)
        
        test.validate()
        if test.validation.all_passed():
            status = True 
            print("The validation checks passed")
            if save:
                print("Saving the appended data")
                df.to_csv(data_path, index=False)
        else:
            status = False
            print("The validation checks failed, please check the log")

        self.local_data = d1
        self.new_data = d2
        self.appended_data = df
        self.status = status
        self.save = save
        self.validation = test.validation

class AppendLog:
    def __init__(self):
        pass
    def append_log(self, log_path, new_log, save=False):
        """
        Append new log data to an existing log CSV file.

        Parameters:
        log_path (str): The file path to the existing log CSV.
        new_log (dict): The new log data to append.
        save (bool, optional): Whether to save the appended log data back to the CSV file. Defaults to False.

        Returns:
        None

        Attributes:
        new_log (dict): The new log data to append.
        log (pd.DataFrame): The combined log data after appending.
        status (bool): The status of the append operation.
        save (bool): Whether the log data was saved back to the CSV file.
        """
        if check_file_exists(log_path):
            print("Loading the log file")
            log = pd.read_csv(log_path)
            log["time"] = pd.to_datetime(log["time"])
            log["start"] = pd.to_datetime(log["start"])
            log["end"] = pd.to_datetime(log["end"])
            log["start_act"] = pd.to_datetime(log["start_act"])
            log["end_act"] = pd.to_datetime(log["end_act"])
            new_log["index"] = log["index"].max() + 1
            output = pd.concat([log, pd.DataFrame([new_log])])
        else:
            print("Could not find a log file on the current path, creating new one")
            new_log["index"] = 1
            output = pd.DataFrame([new_log])
        # Add validation step
        status = True  
        
        if save:
            output.to_csv(log_path, index=False)

        self.new_log = new_log
        self.log = output
        self.status = status
        self.save = save