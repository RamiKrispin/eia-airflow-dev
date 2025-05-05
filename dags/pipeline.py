import sys
sys.path.append("/workspaces/eia-airflow-dev")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
import etl.eia_etl as ee
import etl.callable as cl
import os
import json
import pointblank as pb

# Settings
settings_path = "./settings/settings.json"
api_key_var = "EIA_API_KEY"
os.environ["settings_path"] =  "./settings/settings.json"
raw_json = open(settings_path)
settings = json.load(raw_json)
save = True

forecast_schema = pb.Schema(
    columns=[
        ("unique_id", "object"),
        ("ds", "datetime64[ns]"),   
        ("model_label", "object"),
        ("forecast", "float64"),
        ("lower", "float64"),
        ("upper", "float64"),
        ("forecast_label", "object")
    ]
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'data_pipeline',
    default_args=default_args,
    description='Data pipeline for ETL process',
    schedule_interval='@daily',
    tags = ["python", "etl", "forecast"]
) as dag:
    check_api = PythonOperator(
        task_id='check_api',
        python_callable=cl.check_updates_api,
        op_kwargs={"path": settings_path, "var": api_key_var}   
        )
    check_status = BranchPythonOperator(
        task_id='check_status',
        python_callable=cl.update_status,
        provide_context=True
        )
    data_refresh = BranchPythonOperator(
        task_id='data_refresh',
        python_callable=cl.data_refresh,
        provide_context=True,
        op_kwargs={"var": api_key_var}
        )
    no_updates = PythonOperator(
        task_id='no_updates',
        python_callable=cl.no_updates,
        provide_context=True,
        op_kwargs={"save": save}
        )
    data_validation =  PythonOperator(
        task_id='data_validation',
        python_callable=cl.data_validation,
        provide_context=True
        )
    check_validation = BranchPythonOperator(
        task_id='check_validation',
        python_callable=cl.check_validation,
        provide_context=True
        )
    data_valid = PythonOperator(
        task_id='data_valid',
        python_callable=cl.data_valid,
        provide_context=True,
        op_kwargs={"save": save}
        )
    data_invalid = PythonOperator(
        task_id='data_invalid',
        python_callable=cl.data_invalid,
        provide_context=True
        )
    failure = PythonOperator(
        task_id='data_failure',
        python_callable=cl.data_invalid,
        provide_context=True
        )
    forecast_refresh = PythonOperator(
        task_id='forecast_refresh',
        python_callable=cl.forecast_refresh,
        provide_context=True,
        op_kwargs= {"settings_path": settings_path,
                    "schema": forecast_schema,
                    "save": True,
                    "initial": False}
        )
    forecast_score = PythonOperator(
        task_id='forecast_score',
        python_callable=cl.score,
        op_kwargs= {"settings_path": settings_path,
                    "save": save},
        provide_context=True
        )
check_api >> check_status >> [data_refresh, no_updates]
data_refresh >> [data_validation,failure] 
data_validation >> check_validation >> [data_valid, data_invalid]
data_valid >> forecast_refresh >> forecast_score