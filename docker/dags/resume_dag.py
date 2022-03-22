"""Resume redshift cluster."""
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_cluster import RedshiftResumeClusterOperator
from dotenv import load_dotenv
import os
load_dotenv()

default_args = {
    'owner': 'Mike',
    'start_date': datetime(2019, 1, 12),
    'depents_on_past': False,
    'email_on_retry': False,
    'catchup_by_default': False
}

dag3 = DAG(
    dag_id='a_resume',
    default_args=default_args,
    description='a_resume',
    schedule_interval=None)

resume_cluster = RedshiftResumeClusterOperator(
    task_id='resume_cluster',
    dag=dag3,
    cluster_identifier = 'redshift-cluster-1',
    aws_conn_id='aws_credentials'
)
