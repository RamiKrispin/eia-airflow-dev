import pandas as pd
import pointblank as pb
import datetime
import os
import json
# from numpy import mean
from mlforecast import MLForecast
from mlforecast.target_transforms import Differences
from mlforecast.utils import PredictionIntervals
from window_ops.expanding import expanding_mean
from lightgbm import LGBMRegressor
from utilsforecast.plotting import plot_series
from statistics import mean



class FcSettings:
    def __init__(self):
        pass
    def get_forecast_settings(self, settings_path):
        raw_json = open(settings_path)
        settings = json.load(raw_json)
        forecast_model = settings["forecast"]["model"]

        args = forecast_model["args"]
        if "lags" not in args.keys():
            args["lags"] = None
        if "date_features" not in args.keys():
            args["date_features"] = None
        if "target_transforms" not in args.keys():
            args["target_transforms"] = None

        model_settings = {
            "data_path": settings["data"]["data_path"],
            "data_log_path" :settings["data"]["data_log_path"],
            "forecast_path": settings["forecast"]["settings"]["forecast_path"],
            "forecast_log_path": settings["forecast"]["settings"]["forecast_log_path"],
            "unique_id": settings["api"]["facets"]["respondent"],
            "model": forecast_model["args"]["models"][0],
            "model_label": forecast_model["label"],
            "forecast_label": None,
            "forecast_start": None,
            "lags": args["lags"],
            "freq": args["freq"],
            "date_features": args["date_features"],
            "target_transforms": args["target_transforms"],
            "h": settings["forecast"]["settings"]["h"],
            "refresh": settings["forecast"]["settings"]["refresh"],
            "pi": settings["forecast"]["settings"]["prediction_intervals"]
        }
        self.settings = model_settings




class FcValidation:
    def __init__(self):
        pass
    
    def add_settings(self, fc, schema, 
    settings, tbl_name, label, 
    warning=0, error=0, critical=0):
        self.fc = fc
        self.schema = schema
        self.settings = settings
        self.tbl_name = tbl_name
        self.label = label
        self.warning = warning
        self.error = error
        self.critical = critical

    def validate(self):
        validation = (
            pb.Validate(data = self.fc,
            tbl_name= self.tbl_name,
            label = self.label,
            thresholds=pb.Thresholds(warning=self.warning, error=self.error, critical=self.critical))
            .col_schema_match(schema=self.schema)
            .col_vals_gt(columns="forecast", value=0)
            .col_vals_gt(columns="upper", value=0)
            .col_vals_gt(columns="lower", value=0)
            .col_count_match(count=len(self.schema.columns)) 
            .col_vals_in_set(columns="unique_id", set = [self.settings["unique_id"]])
            .col_vals_in_set(columns="forecast_label", set = [self.settings["forecast_label"]])
            .col_vals_not_null(columns= ["ds","forecast", "lower", "upper"])
            .row_count_match(count = self.settings["h"])
            .rows_distinct() 
            .interrogate()
        )
        self.validation = validation
        self.status = validation.all_passed()


        log = {
            "index": None,
            "unique_id":  self.settings["unique_id"],
            "model": self.settings["model"],
            "model_label": self.settings["model_label"],
            "forecast_label": self.settings["forecast_label"],
            "forecast_start": self.settings["forecast_start"],
            "forecast_start_act": self.fc["ds"].min(),
            "forecast_end_act": self.fc["ds"].max(),
            "n_obs": len(self.fc),
            "h": self.settings["h"],
            "refresh_time": datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "start_flag": None,
            "h_flag": None,
            "validation": None,
            "status": None,
            "update": None,
            "mape": None,
            "rmse": None,
            "coverage": None,
            "score": False,
            "comments": ""
            }

        if self.status:
            log["validation"] = True
        else:
            log["validation"] = False 
            log["comments"] = "The validation process failed; "

        if log["n_obs"] == log["h"]:
            log["h_flag"] = True
        else:
            log["h_flag"] = False
            log["comments"] = "Mismatch between the forecast horizon and number of observations; "

        if log["forecast_start"] == log["forecast_start_act"]:
            log["start_flag"] = True
        else:
            log["start_flag"] = False
            log["comments"] = "Mismatch between the forecast start argument and the actual start value; "

        if log["validation"] & log["h_flag"] & log["start_flag"]:
            log["status"] = True
        else:
            log["status"] = False
        
        
        self.log = log
                

def check_file_exists(file_path):
    """
    Check if a file exists at the given file path.

    Parameters:
    file_path (str): The path to the file.

    Returns:
    bool: True if the file exists, False otherwise.
    """
    return os.path.isfile(file_path)

class AppendFcLog:
    def __init__(self):
        pass
    def append_log(self, settings_path, new_log, save=False):
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
        fc_settings = FcSettings()
        fc_settings.get_forecast_settings(settings_path=settings_path)
        s = fc_settings.settings
        if check_file_exists(s["log_path"]):
            print("Loading the log file")
            log = pd.read_csv(s["log_path"])
            log["forecast_start"] = pd.to_datetime(log["forecast_start"])
            log["forecast_start_act"] = pd.to_datetime(log["forecast_start_act"])
            log["forecast_end_act"] = pd.to_datetime(log["forecast_end_act"])
            new_log["index"] = log["index"].max() + 1
            output = pd.concat([log, pd.DataFrame([new_log])])
        else:
            print("Could not find a log file on the current path, creating new one")
            new_log["index"] = 1
            output = pd.DataFrame([new_log])
        
        if save:
            output.to_csv(s["log_path"], index=False)

        self.new_log = new_log
        self.log = output
        self.save = save


class AppendForecast:
    pass

    def append_forecast(self, settings_path, new_fc, schema,save=False, initial = False):
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
        
        fc_settings = FcSettings()
        fc_settings.get_forecast_settings(settings_path=settings_path)
        s = fc_settings.settings
        if not initial:
            if check_file_exists(s["forecast_path"]):
                print("Loading the forecast file")
                fc = pd.read_csv(s["forecast_path"])
                fc["ds"] = pd.to_datetime(fc["ds"])
                output = pd.concat([fc, new_fc])
                output = output.sort_values(by="ds")
                output.reset_index(drop=True, inplace=True)
            else:
                print("Could not find a forecast file on the current path, creating new one")
                output = new_fc
        elif initial:
            print("Initiating a new forecast file")
            output = new_fc


        validation = (
            pb.Validate(data = output,
            tbl_name= "appended_forecast",
            label = "appended forecast",
            thresholds=pb.Thresholds(warning=0, error=0, critical=0))
            .col_schema_match(schema = schema)
            .col_vals_gt(columns="forecast", value = 0)
            .col_vals_gt(columns="upper", value = 0)
            .col_vals_gt(columns="lower", value = 0)
            .col_count_match(count=len(schema.columns)) 
            .col_vals_in_set(columns="unique_id", set = [s["unique_id"]])
            .col_vals_not_null(columns= ["ds","forecast", "lower", "upper"])
            .rows_distinct() 
            .interrogate()
        )

        self.validation = validation
        self.status = validation.all_passed()
        if validation.all_passed():
            print("The forecast validation completed successfully")
            status = True
        else:
            print("The forecast validation failed, please check the log")
            status = False 
        
        if save:
            print("Saving the appended data")
            output.to_csv(s["forecast_path"], index=False)
        else:
            print("The forecast was not saved")
            
        self.new_fc = new_fc
        self.appended_fc = output
        self.status = status
        self.save = save



class AppendFcLog:
    def __init__(self):
        pass
    
    def append_log(self, settings_path, new_log, save=False, initial=False):
        """
        Append new log data to an existing log CSV file.

        Parameters:
        forecast_log_path (str): The file path to the existing log CSV.
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
        fc_settings = FcSettings()
        fc_settings.get_forecast_settings(settings_path=settings_path)
        s = fc_settings.settings
        if not initial:
            if check_file_exists(s["forecast_log_path"]):
                print("Loading the log file")
                log = pd.read_csv(s["forecast_log_path"])
                log["forecast_start"] = pd.to_datetime(log["forecast_start"])
                log["forecast_start_act"] = pd.to_datetime(log["forecast_start_act"])
                log["forecast_end_act"] = pd.to_datetime(log["forecast_end_act"])
                new_log["index"] = log["index"].max() + 1
                print("XXX")
                print(new_log)
                print(pd.DataFrame([new_log]))
                print(s["forecast_log_path"])
                print(log)

                print(log.dtypes)


                output = pd.concat([log, pd.DataFrame([new_log])]).reset_index(drop=True, inplace=False)
                print(output)
                print(type(output))
            else:
                print("Could not find a log file on the current path, creating new one")
                new_log["index"] = 1
                output = pd.DataFrame([new_log])
        else:
            print("Initiating a new log file")
            new_log["index"] = 1
            output = pd.DataFrame([new_log])
        # Add validation step
        status = True  
        
        if save:
            output.to_csv(s["forecast_log_path"], index=False)

        self.new_log = new_log
        self.log = output
        self.status = status
        self.save = save


class RefreshForecast:
    def __init__(self):
        pass

    def check_status(self, settings_path, initial = False, start = None):
        s = FcSettings()
        s.get_forecast_settings(settings_path=settings_path)
        self.refresh = s.settings["refresh"]
        forecast_log_path = s.settings["forecast_log_path"]
        data_log_path = s.settings["data_log_path"]
        self.settings = s.settings

        if initial:
            log = pd.read_csv(data_log_path)
            log["end_act"] = pd.to_datetime(log["end_act"])
            log = log[log["status"] == True]
            log = log[log["update"] == True]
            log["end_act"] = pd.to_datetime(log["end_act"])
            if start <= log["end_act"].max():
                print("Initializing the forecast")
                self.start = start
                self.settings["forecast_label"] = str(start)
                self.settings["forecast_start"] = start    
                self.status = True
            else:
                self.status = False
                print("The start argument is not aligned with the data end timestamp")
        else:
            if check_file_exists(forecast_log_path) and check_file_exists(data_log_path):
                print("Loading the log files")
                log = pd.read_csv(data_log_path)
                log["end_act"] = pd.to_datetime(log["end_act"])
                log = log[log["status"] == True]
                log = log[log["update"] == True]
                self.end_act = log["end_act"].max()
                log_fc = pd.read_csv(forecast_log_path)
                log_fc["forecast_start"] = pd.to_datetime(log_fc["forecast_start"])
                log_fc["forecast_start_act"] = pd.to_datetime(log_fc["forecast_start_act"])
                log_fc["forecast_end_act"] = pd.to_datetime(log_fc["forecast_end_act"])
                start = log_fc["forecast_start"].max() + datetime.timedelta(hours = self.refresh)
                self.settings["forecast_label"] = str(start)
                self.settings["forecast_start"] = start   
                self.start = start
                if self.start < self.end_act:
                    self.status = True
                else:
                    print("No new data is available to refresh the forecast")
                    self.status = False
            else:
                print("No log file found")
                self.start = None
                self.status = False

            
    def refresh_forecast(self):
        data_path = self.settings["data_path"]
        if check_file_exists(data_path):
            print("Loading the data file")
            data = pd.read_csv(data_path)
            data["index"] = pd.to_datetime(data["index"])
            ts = data[["index", "respondent", "value"]]
            ts = ts.rename(columns={"index": "ds", "value": "y", "respondent": "unique_id"})
            ts = ts.sort_values(by="ds")
            ts = ts[ts["ds"] < self.start]

            model = {
                self.settings["model_label"]: eval(self.settings["model"])
                }
            mlf = MLForecast(
                models = model,
                freq = self.settings["freq"],
                date_features = self.settings["date_features"],
                target_transforms = self.settings["target_transforms"],
                lags = self.settings["lags"]
            )

            mlf.fit(df = ts, 
                    fitted=True, 
                    prediction_intervals = PredictionIntervals(n_windows = self.settings["pi"]["n_windows"], 
                    h = self.settings["pi"]["h"],
                    method = self.settings["pi"]["method"]))
            self.raw_forecast = mlf.predict(h = self.settings["h"], level  =[self.settings["pi"]["level"]])
    def reformat_forecast(self):
        fc_long = pd.melt(
                    self.raw_forecast,
                    id_vars=["unique_id", "ds"],
                    value_vars=[self.settings["model_label"]] +[f"{model }-lo-95" for model in [self.settings["model_label"]]] \
                    				  + [f"{model}-hi-95" for model in [self.settings["model_label"]]],
                    var_name="model_label",
                    value_name="value")
        fc_long["model_label"],\
        fc_long["type"] = zip(*fc_long["model_label"].map(split_model_confidence))
        forecast = (fc_long
                .pivot(index = ["unique_id", "ds", "model_label"], 
                columns = "type", values = "value")
                .reset_index())

        forecast["forecast_label"] = str(self.start)
        self.forecast = forecast
    def add_schema(self, schema):
        self.schema = schema
    def forecast_validation(self):
        v = FcValidation()
        v.add_settings(fc = self.forecast, schema = self.schema, settings = self.settings, 
                tbl_name = "forecast", label = "forecast", warning=0, error=0, critical=0)
        v.validate()
        self.validation = v

    
        

def split_model_confidence(model_name):
    if "-lo-95" in model_name:
        return model_name.replace("-lo-95", ""), "lower"
    elif "-hi-95" in model_name:
        return model_name.replace("-hi-95", ""), "upper"
    else:
        return model_name, "forecast"   

def score_forecast(settings_path, save = False):
    
    s = FcSettings()
    s.get_forecast_settings(settings_path=settings_path)
    forecast_log_path = s.settings["forecast_log_path"]
    forecast_path = s.settings["forecast_path"]
    data_path = s.settings["data_path"]

    if check_file_exists(forecast_log_path) and \
    check_file_exists(forecast_path) and \
        check_file_exists(data_path):
        changes = False
        log = pd.read_csv(forecast_log_path)
        f_log = log[(log["update"] == True) & (log["score"] == False)]
        f_log["forecast_start_act"] = pd.to_datetime(f_log["forecast_start_act"])
        f_log["forecast_end_act"] = pd.to_datetime(f_log["forecast_end_act"])
        if len(f_log) > 0:
            df = pd.read_csv(data_path)
            df["index"] = pd.to_datetime(df["index"])
            df = df.rename(columns= {"index": "ds", "value": "y" })
            df = df[["ds", "y"]]

            fc = pd.read_csv(forecast_path)
            fc["ds"] = pd.to_datetime(fc["ds"])
            end = df["ds"].max()
            for index, row in f_log.iterrows():
                forecast_label = row["forecast_label"]
                index = row["index"]
                f = fc[fc["forecast_label"] == forecast_label].merge(df, how = "left", on = "ds")
                f = f.dropna() 
                log.loc[log["forecast_label"] == forecast_label, "mape"] = mape(y = f["y"], yhat = f["forecast"])
                log.loc[log["forecast_label"] == forecast_label, "rmse"] = rmse(y = f["y"], yhat = f["forecast"])
                log.loc[log["forecast_label"] == forecast_label, "coverage"] = coverage(y = f["y"], lower = f["lower"], upper = f["upper"])
                changes = True
                if len(f) == row["n_obs"]:
                    log.loc[log["forecast_label"] == forecast_label, "score"] = True
            
            if changes & save:
                log.to_csv(forecast_log_path, index=False)
    return log

def mape(y, yhat):
    mape = mean(abs(y - yhat)/ y) 
    return mape

def rmse(y, yhat):
    rmse = (mean((y - yhat) ** 2 )) ** 0.5
    return rmse

def coverage(y, lower, upper):
    coverage = sum((y <= upper) & (y >= lower)) / len(y)
    return coverage