import etl.dags_functions as df
import etl.forecast_utils as fu
import etl.eia_etl as ee
import pandas as pd
import pointblank as pb
import json



def check_updates_api(path, var):
    check = df.CheckUpdates()
    check.check_updates(path = path, var = var)
    return check.status


def update_status(**context):
    parameters = context['ti'].xcom_pull(task_ids='check_api')
    if parameters["updates_available"] == "True":
        return "data_refresh"
    else:
        return "no_updates"

def data_refresh(var, **context):
    settings = df.Settings()
    settings.load_api_key(var = var)
    parameters = context['ti'].xcom_pull(task_ids='check_api')
    print("Data refresh")
    print(parameters)
    data = df.DataRefresh()
    data.refresh_data(var = var, parameters = parameters)
    if len(data.data) > 0:
        data.data.to_csv(parameters["data_validation_path"], index = False) 
        return "data_validation"
    else:
        return "data_failure"

    
def data_validation(**context):
    parameters = context['ti'].xcom_pull(task_ids='check_api')
    data_validation_path = parameters["data_validation_path"] 
    log = ee.Log()
    log.create_log(facets = parameters["facets"])
    df = pd.read_csv(data_validation_path)
    df["index"] = pd.to_datetime(df["index"])
    print(df.dtypes)

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
    data_validation = ee.Validation(data = df,  
        tbl_name= "get request",
        label = "validation",
        parameters= parameters,
        initial= False,
        warning=0.10, 
        error=0, 
        critical=0)
    data_validation.add_schema(schema = schema_refresh)
    data_validation.validate()
    print("Data validation")
    print(data_validation.validation.all_passed())
    if data_validation.validation.all_passed():
        print(data_validation.parameters)
        log.add_validation(validation= data_validation)
        print(log.log)
        log.log["time"] = str(log.log["time"])
        log.log["start"] = str(log.log["start"])
        log.log["end"] = str(log.log["end"])
        log.log["start_act"] = str(log.log["start_act"])
        log.log["end_act"] = str(log.log["end_act"])
        print(log.log)
        return log.log
    else:
        log.fauilure() 
        return log.log

def check_validation(**context):
    log = context['ti'].xcom_pull(task_ids="data_validation")
    if log["validation"] == True:
        return "data_valid" 
    else:
        return "data_invalid"
    

def no_updates(save, **context):
    print("No updates available")
    parameters = context['ti'].xcom_pull(task_ids='check_api')
    facets = parameters["facets"] 
    log = ee.Log()
    log.create_log(facets = facets)
    log.no_updates()
    ee.AppendLog().append_log(log_path = parameters["log_path"], 
                              new_log = log.log, save = save)
    return log.log


def data_invalid(save, **context):
    print("Data is invalid.")
    parameters = context['ti'].xcom_pull(task_ids='check_api')
    facets = parameters["facets"] 
    log = ee.Log()
    log.create_log(facets = facets)
    log.fauilure()

    ee.AppendLog().append_log(log_path = parameters["log_path"], 
                              new_log = log.log, save = save)
    return log.log
    

def data_valid(save, **context):
    print("Appending and saving the data")
    parameters = context['ti'].xcom_pull(task_ids='check_api')
    data_validation_path = parameters["data_validation_path"] 
    log = context['ti'].xcom_pull(task_ids="data_validation")
    
    schema_append = pb.Schema(
        columns=[
        ("index", "datetime64[ns]"),   
        ("respondent", "object"),
        ("type", "object"),
        ("value", "int64"),
        ("value-units", "object")
        ]
    )
    df = pd.read_csv(data_validation_path)
    df["index"] = pd.to_datetime(df["index"])
    data = ee.AppendData()
    data.append_data(data_path = parameters["data_path"], 
                    new_data = df,
                    schema = schema_append,
                    parameters = parameters, 
                    save = save)
    
    if data.status and save:
        log["update"] = True
    elif not data.status:
        log["update"] = False
        log["comments"] = "Data append failed; "
    elif not save:
        log["comments"] = "Data not saved; "
        log["update"] = False
    print(log)
    new_log = ee.AppendLog()
    new_log.append_log(log_path = parameters["log_path"], 
                                  new_log = log, save = save)
    print(new_log.new_log)  
    print(new_log.status) 
    print(new_log.log) 
    return new_log.new_log

    

def forecast_refresh(settings_path, schema, save, initial, start = None):
    fc_settings = fu.FcSettings()
    fc_settings.get_forecast_settings(settings_path=settings_path)
    s = fc_settings.settings
    f = fu.RefreshForecast()
    f.check_status(settings_path=settings_path, initial=initial, start=start)
    if f.status:
        print("New data is available to refresh the forecast")
        f.refresh_forecast()
        f.reformat_forecast()
        f.add_schema(schema = schema)
        f.forecast_validation()
        log = f.validation.log
        if f.validation.log["status"]:
            af = fu.AppendForecast()
            af.append_forecast(settings_path = settings_path, 
                               new_fc = f.forecast, 
                               schema = schema,save = save, 
                               initial= initial)
            if af.save:
                if af.status:
                    log["update"] = True
                else:
                    log["update"] = False
                    log["comments"] = "Failed to append the forecast, please check the validation log"
                al = fu.AppendFcLog()
                al.append_log(settings_path = settings_path, new_log = log, save = save, initial = initial)   
            else:
                print("The save argument was set to False, skipping the log process")
        print(log)
        return True
    else:
        print("No new data is available to refresh the forecast")
        return False


def score(settings_path, save=False):
    log = fu.score_forecast(settings_path = settings_path, save = save)
    print(log)
    return True