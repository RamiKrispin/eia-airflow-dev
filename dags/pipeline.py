import sys
sys.path.append("/workspaces/eia-airflow-dev")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from callable.check_updates import CheckUpdates, HelloWorld, DataRefresh
import etl.eia_etl as ee
import etl.callable as cl
import os

# Settings
settings_path = "./settings/settings.json"
api_key_var = "EIA_API_KEY"
os.environ["settings_path"] =  "./settings/settings.json"

def update_status(**context):
    parameters = context['ti'].xcom_pull(task_ids='check_api')
    print(parameters)
    if parameters["updates_available"] == "True":
        return "updates_available"
    else:
        return "no_updates"

def print_env_var():
    print(os.environ["settings_path"])
    print(os.environ["EIA_API_KEY"])
    
def process_json_data(path, var, **context):
    # Access the returned JSON data from XCom
    parameters = context['ti'].xcom_pull(task_ids='check_updates')
    print(type(parameters))
    print(parameters)
    data_log = ee.Log()
    data_log.create_log(facets = parameters["facets"])
    if parameters["updates_available"] == "True":
        print("Data is available.")
        print(parameters["start"])
        data = DataRefresh()
        data.refresh_data(parameters = parameters)
        data.data_validation()
        if data.validation.all_passed():
            print("Data is valid.")
            data_log.add_validation(validation = data.validation)
            print(data_log.log)
            
        
    else:
        print("No data available.")
    


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
    schedule_interval=None,
    tags = ["python1", "test1"]
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
        op_kwargs={"save": True}
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
        op_kwargs={"save": True}
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
check_api >> check_status >> [data_refresh, no_updates]
data_refresh >> [data_validation,failure] 
data_validation >> check_validation >> [data_valid, data_invalid]