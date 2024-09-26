from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 1),  # adjust the date as needed
    'retries': 1,
}

# Define the function that will be executed by the PythonOperator
def hello_world():
    print("Hello World!")

# Instantiate the DAG
with DAG(
    'hello_world_dag_k8s_executor',
    default_args=default_args,
    description='A simple hello world DAG on Kubernetes Executor',
    schedule_interval='* * * * *',  # Runs every minute
    catchup=False
) as dag:

    # Create a task using the PythonOperator
    hello_world_task = PythonOperator(
        task_id='print_hello_world',
        python_callable=hello_world
    )

# Define task dependencies (optional if you have more tasks)
hello_world_task
