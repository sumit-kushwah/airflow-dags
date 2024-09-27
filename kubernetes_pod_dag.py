from airflow import DAG
from datetime import datetime
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
import airflow.providers.cncf.kubernetes as k8s

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'custom_docker_image_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust the schedule as needed
    catchup=False,
) as dag:

    # Define the KubernetesPodOperator task
    run_custom_docker_task = KubernetesPodOperator(
        namespace='airflow',  # Namespace where the pod will be created
        image='hello-world:latest',  # Replace with your custom Docker image
        arguments=["print('Hello from custom Docker image!')"],
        name="hello-world-task",
        in_cluster=True,  # If you're running the task within the Kubernetes cluster
        is_delete_operator_pod=True,  # Clean up the pod after task completes
        get_logs=True,  # Retrieve logs from the pod
    )

# The task will run when the DAG is executed
run_custom_docker_task
