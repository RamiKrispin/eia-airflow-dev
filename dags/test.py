from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


def print_hello():
    return 'Hello world!'

def sum(a, b):
    print(a + b)
    return a + b

def multiply(a, b):
    print(a * b)
    return a * b

def print_values(ti):
    a = ti.xcom_pull(task_ids='sum_task')
    b = ti.xcom_pull(task_ids='multiply_task')
    print("Sum:", a)
    print("Multiply:", b)

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
    'hello_world',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=None,
    tags = ["python", "test"]
) as dag:
    python_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello
        )
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "Hello World"'
    )
    python_task2 = PythonOperator(
        task_id='sum_task',
        python_callable=sum,
        op_args=[2, 3]
    )
    python_task3 = PythonOperator(
        task_id='multiply_task',
        python_callable=multiply,
        op_args=[2, 3]
    )
    python_task4 = PythonOperator(
        task_id='print_results_task',
        python_callable=print_values
    )


# python_task >> bash_task1
# python_task >> bash_task2

# bash_task1 >> bash_task3
# bash_task2 >> bash_task3

python_task >> [python_task2, python_task3] >> python_task4