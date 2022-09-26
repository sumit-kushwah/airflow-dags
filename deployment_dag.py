import pendulum, datetime, boto3
from airflow.decorators import dag, task
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models.baseoperator import chain
from airflow.models import Variable

@dag(
    dag_id='deployment_dag',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)
def DeploymentPipeline():
    pull_latest_model = SSHOperator(
        task_id='pull_latest_production_model',
        ssh_conn_id='ssh_deploy_server',
        command="source /home/sumitkushwah1729/miniconda3/etc/profile.d/conda.sh && \
            conda activate flask_api_deployment && \
            cd /home/sumitkushwah1729/ml_flask_api/models && \
            python pull_latest_model.py \
            --model_name fasttext_product_non_pharma \
            --tracking_uri {{var.value.get('mlflow_tracking_uri')}} \
            --get_model True",
    )

    restart_service = SSHOperator(
        task_id='restart_flask_api',
        ssh_conn_id='ssh_deploy_server',
        command="sudo systemctl restart flask_api.service",
    )

    chain(
        pull_latest_model,
        restart_service
    )

dag = DeploymentPipeline()
