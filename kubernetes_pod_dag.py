from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

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
        namespace='default',  # Namespace where the pod will be created
        image='hello-world:latest',  # Replace with your custom Docker image
        cmds=["python", "-c"],
        arguments=["print('Hello from custom Docker image!')"],
        name="run-custom-docker-task",
        task_id="run_custom_docker_task",
        in_cluster=True,  # If you're running the task within the Kubernetes cluster
        is_delete_operator_pod=True,  # Clean up the pod after task completes
        get_logs=True,  # Retrieve logs from the pod
        resources={
            "request_memory": "200Mi",  # Request 200MB of memory
            "request_cpu": "500m",      # Request 0.5 CPU
            "limit_memory": "200Mi",    # Limit memory usage to 200MB
            "limit_cpu": "500m",        # Limit CPU usage to 0.5 CPU
        },
    )

# The task will run when the DAG is executed
run_custom_docker_task
