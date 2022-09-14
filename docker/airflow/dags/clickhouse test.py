from airflow.decorators import dag
from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator

@dag(
    dag_id="trip_etl",
    schedule_interval='@daily',     
    start_date=datetime(2022, 1, 1),
    catchup=False,
    )
def ssh_dag():
    task_1=SSHOperator(
        task_id="ssh_task",
        ssh_conn_id='nfrssh',
        command="cd C:/Users/nfrs/Documents/Jupyter/NetologyDiplom/ && bin.bat"
    )

my_ssh_dag = ssh_dag()


