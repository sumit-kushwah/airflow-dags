import pendulum, datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.ec2 import EC2StartInstanceOperator, EC2StopInstanceOperator

@dag(
    dag_id='training_dag',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)

def TrainingPipeline():
    start_instance = EC2StartInstanceOperator(
        task_id="ec2_start_instance",
        instance_id='i-0a0771b65f73ec465',
    )


dag = TrainingPipeline()
