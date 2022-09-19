import pendulum, datetime, boto3
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.ec2 import EC2StartInstanceOperator, EC2StopInstanceOperator
from airflow.providers.amazon.aws.sensors.ec2 import EC2InstanceStateSensor
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models.baseoperator import chain
from airflow.models import Variable


instance_id = Variable.get('instance_id')

def find_instance_ip():
    ec2 = boto3.resource('ec2')
    instance = ec2.Instance(instance_id)
    return instance.public_ip_address

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
        instance_id=instance_id,
    )

    instance_state = EC2InstanceStateSensor(
        task_id="ec2_instance_state",
        instance_id=instance_id,
        target_state="running",
    )

    instance_ip = PythonOperator(
        task_id='find_instance_ip',
        python_callable= find_instance_ip,
        op_kwargs = {"x" : "Apache Airflow"},
    )

    ssh_connect = SSHOperator(
        task_id='instance_connect',
        ssh_conn_id='ssh_default',
        command='pwd',
        remote_host="{{ ti.xcom_pull(task_ids='find_instance_ip') }}"
    )

    stop_instance = EC2StopInstanceOperator(
        task_id="ec2_stop_instance",
        instance_id=instance_id,
    )

    chain(
        start_instance,
        instance_state,
        instance_ip,
        ssh_connect,
        stop_instance
    )


dag = TrainingPipeline()
