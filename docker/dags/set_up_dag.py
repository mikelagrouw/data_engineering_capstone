"""Set up connections and create tables."""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import operators.create_airflow_connections as create_airflow_connections
import helpers.create_statements as create_statements
from configuration import Configuration
import os
from dotenv import load_dotenv



default_args = {
    'owner': 'Mike',
    'start_date': datetime(2019, 1, 12),
    'depents_on_past': False,
    'email_on_retry': False,
    'catchup_by_default': False
}

dag = DAG(
    dag_id='a_Set_up',
    default_args=default_args,
    description='Capstone_setup_dag',
    schedule_interval=None
)

start = DummyOperator(
    task_id='begin_execution',
    dag=dag
)

too_create = DummyOperator(
    task_id='begin_creating',
    dag=dag
)

too_fact_dim = DummyOperator(
    task_id='fact_dim_tables',
    dag=dag
)


create_aws_connection = create_airflow_connections.CreateConnections(
    task_id='create_aws_connection',
    dag=dag,
    conn_id='aws_credentials',
    conn_type='Amazon Web Services',
    login=os.getenv('aws_key'),
    pwd=os.getenv('aws_pwd')
)
create_redshift_connection = create_airflow_connections.CreateConnections(
    task_id='create_redshift_connection',
    dag=dag,
    conn_id='redshift',
    conn_type='Postgresql',
    login=os.getenv('redshift_user'),
    pwd=os.getenv('redshift_pwd'),
    port=os.getenv('port'),
    host=os.getenv('host'),
    schema=os.getenv('schema')
)
create_immigration_staging_table = PostgresOperator(
    task_id='create_immigration_staging_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_statements.CreateStatements.create_staging_immigration
)

create_country_staging_table = PostgresOperator(
    task_id='create_country_staging_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_statements.CreateStatements.create_staging_country
)

create_country_mapping_table = PostgresOperator(
    task_id='create_country_mapping_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_statements.CreateDim.create_mapping_country
)

create_demographic_staging_table = PostgresOperator(
    task_id='create_demographic_staging_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_statements.CreateStatements.create_staging_demographics
)

create_temperature_staging_table = PostgresOperator(
    task_id='create_temperature_staging_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_statements.CreateStatements.create_staging_temperature
)

create_airport_staging_table = PostgresOperator(
    task_id='create_airport_staging_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_statements.CreateStatements.create_staging_airports
)

create_fact_i94 = PostgresOperator(
    task_id='create_fact_i94_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_statements.CreateFact.create_fact_i94
)

create_dimension_demographics = PostgresOperator(
    task_id='create_dimension_demographics',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_statements.CreateDim.create_dim_demographics
)

create_dimension_airport = PostgresOperator(
    task_id='create_dimension_airport',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_statements.CreateDim.create_dim_airport
)

create_dimension_time = PostgresOperator(
    task_id='create_dimension_time',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_statements.CreateDim.create_dim_time
)

create_dimension_temp = PostgresOperator(
    task_id='create_dimension_temp',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_statements.CreateDim.create_dim_climate
)

start >> [create_aws_connection, create_redshift_connection]
[create_aws_connection, create_redshift_connection] >> too_create
too_create >> [create_immigration_staging_table,
               create_demographic_staging_table,
               create_temperature_staging_table,
               create_airport_staging_table,
               create_country_staging_table]

create_immigration_staging_table >> too_fact_dim
create_demographic_staging_table >> too_fact_dim
create_temperature_staging_table >> too_fact_dim
create_airport_staging_table >> too_fact_dim
create_country_staging_table >> too_fact_dim

too_fact_dim >> [create_fact_i94,
                 create_dimension_demographics,
                 create_dimension_airport,
                 create_dimension_time,
                 create_dimension_temp,
                 create_country_mapping_table]
