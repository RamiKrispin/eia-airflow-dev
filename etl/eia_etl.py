import pandas as pd
import datetime
import etl.eia_api as ea
import pointblank as pb
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
    facets (pd.DataFrame): The series facets DataFrame containing respondent and type.

    Returns:
    Metadata: An object containing the loaded metadata, last index, and request metadata.
    """
    meta = pd.read_csv(path)
    meta["time"] = pd.to_datetime(meta["time"])
    meta["start"] = pd.to_datetime(meta["start"])
    meta["start_act"] = pd.to_datetime(meta["start_act"])
    meta["end"] = pd.to_datetime(meta["end"])
    meta["end_act"] = pd.to_datetime(meta["end_act"])

    log_temp = {
        "index": None,
        "respondent": None,
        "type": None,
        "end_act": None,
        "request_start": None
    }

    meta_success = meta[meta["success"] == True]
    request_meta = pd.DataFrame()

    for i in facets.index:
        r = facets.at[i, "respondent"]
        t = facets.at[i, "type"]
        l = meta_success[(meta_success["respondent"] == r) & (meta_success["type"] == t)]
        l = l[l["index"] == l["index"].max()]
        log = log_temp.copy()
        log["respondent"] = r
        log["type"] = t
        log["end_act"] = l["end_act"].max()
        log["request_start"] = l["end_act"].max() + datetime.timedelta(hours=1)
        log["index"] = i
        request_meta = request_meta._append(pd.DataFrame([log]), ignore_index=True)

    request_meta = request_meta.set_index("index")

    return Metadata(metadata=meta, last_index=meta["index"].max(), request_meta=request_meta)


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
    
    local_end = meta.metadata.at[0,"end_act"]
    
    if window is not None:
        start = local_end - datetime.timedelta(hours=window)
    else:
        start = local_end
        window = 0 

    if api_end > local_end:
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
    def __init__(self, data, tbl_name, label, parameters, warning=0, error=0, critical=0):
        self.data=data
        self.tbl_name=tbl_name
        self.label=label
        self.warning=warning
        self.error=error
        self.critical=critical
        self.parameters=parameters

    def add_schema(self, schema):
        self.schema = schema

    def validate(self):
        validation = (
            pb.Validate(data = self.data,
            tbl_name= self.tbl_name,
            label = self.label,
            thresholds=pb.Thresholds(warning=self.warning, error=self.error, critical=self.critical))
            .col_exists(columns=["period", "respondent", "respondent-name", "type", "type-name", "value", "value-units"]) 
            .col_schema_match(schema=self.schema)
            .col_vals_gt(columns="value", value=0)
            .col_count_match(count=7) 
            .col_vals_in_set(columns="respondent", set = [self.parameters["facets"]["respondent"]])
            .col_vals_in_set(columns="type", set = [self.parameters["facets"]["type"]])
            .col_vals_not_null(columns= ["period","value"])
            .rows_distinct() 
            .interrogate()
        )

        self.validation = validation
        self.status = validation.all_passed()

    def create_log(self, log_path, facets):
        
        metadata = load_metadata(path = log_path, facets = facets)
        meta_old = metadata.metadata

        log = {}
        log["index"] = metadata.last_index + 1
        log["respondent"] = self.parameters["facets"]["respondent"]
        log["type"] = self.parameters["facets"]["type"]
        log["time"] = datetime.datetime.now(datetime.timezone.utc)
        log["start"] = self.parameters["start"]
        log["end"] = self.parameters["end"]
        log["start_act"] = self.data["period"].min()
        log["end_act"] = self.data["period"].max()
        log["start_match"] = log["start"] == log["start_act"]
        log["end_match"] = log["end"] == log["end_act"]
        log["n_obs"]
        
        self.log = log



