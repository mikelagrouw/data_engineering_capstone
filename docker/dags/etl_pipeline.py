"""Stage and load data."""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import operators.s3toredshift as s3toredshift
import operators.load_table as load_table
import helpers.select_statements as select_statements
import operators.quality_operator as quality_operator
from airflow.providers.amazon.aws.operators.redshift_cluster import RedshiftPauseClusterOperator
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

dag2 = DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    description='etl_pipeline',
    schedule_interval=None)


start = DummyOperator(
    task_id='begin_execution',
    dag=dag2
)

staging_done = DummyOperator(
    task_id='staging_done',
    dag=dag2
)

pipeline_executed = DummyOperator(
    task_id='pipeline_executed',
    dag=dag2
)

stage_demographics = s3toredshift.StageToRedshiftOperator(
    task_id='stage_demographics',
    dag=dag2,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_dem",
    s3_bucket="capstonemike",
    s3_key="Data_clean/demographics_data.csv",
    format="CSV",
    ignore_header="region 'us-west-2' IGNOREHEADER 1 EMPTYASNULL BLANKSASNULL"
)

stage_temperature = s3toredshift.StageToRedshiftOperator(
    task_id='stage_temperature',
    dag=dag2,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_temp",
    s3_bucket="capstonemike",
    s3_key="Data_clean/temperature_data.csv",
    format="CSV",
    ignore_header="region 'us-west-2' IGNOREHEADER 1 EMPTYASNULL BLANKSASNULL"
)

stage_airport = s3toredshift.StageToRedshiftOperator(
    task_id='stage_airport',
    dag=dag2,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_air",
    s3_bucket="capstonemike",
    s3_key="Data_clean/airport_data.csv",
    format="CSV",
    ignore_header="region 'us-west-2' IGNOREHEADER 1 EMPTYASNULL BLANKSASNULL"
)

stage_country = s3toredshift.StageToRedshiftOperator(
    task_id='stage_country',
    dag=dag2,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_country",
    s3_bucket="capstonemike",
    s3_key="Data_clean/country.csv",
    format="CSV",
    ignore_header="region 'us-west-2' IGNOREHEADER 1 EMPTYASNULL BLANKSASNULL"
)

stage_immigration = s3toredshift.StageToRedshiftOperator(
    task_id='stage_immigration',
    dag=dag2,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_i94",
    s3_bucket="capstonemike",
    s3_key="Data_clean/i94_unpar/unpar.snappy",
    format="PARQUET",
)

load_fact_i94 = load_table.LoadDimensionOperator(
    task_id='load_fact_i94',
    dag=dag2,
    conn_id='redshift',
    table='fact_i94',
    sql=select_statements.SelectStatements.select_fact_i94,
    truncate=True
)

load_dim_demographics = load_table.LoadDimensionOperator(
    task_id='load_dim_demographics',
    dag=dag2,
    conn_id='redshift',
    table='dim_demographics',
    sql=select_statements.SelectStatements.select_dimension_demographics,
    truncate=True
)

load_dim_airport = load_table.LoadDimensionOperator(
    task_id='load_dim_airport',
    dag=dag2,
    conn_id='redshift',
    table='dim_airport',
    sql=select_statements.SelectStatements.select_dimension_airport,
    truncate=True
)

load_dim_time = load_table.LoadDimensionOperator(
    task_id='load_dim_time',
    dag=dag2,
    conn_id='redshift',
    table='dim_time',
    sql=select_statements.SelectStatements.select_dimension_time,
    truncate=True
)

load_dim_climate = load_table.LoadDimensionOperator(
    task_id='load_dim_climate',
    dag=dag2,
    conn_id='redshift',
    table='dim_climate',
    sql=select_statements.SelectStatements.select_temperature_data,
    truncate=True
)

# not implemented completely
load_mapping_country = load_table.LoadDimensionOperator(
    task_id='load_mapping_country',
    dag=dag2,
    conn_id='redshift',
    table='mapping_country',
    sql=select_statements.SelectStatements.select_mapping_country,
    truncate=True
)

quality_check = quality_operator.DataQualityOperator(
    task_id='quality_check',
    dag=dag2,
    conn_id='redshift',
    tests=[{'SELECT * FROM fact_i94': 'not empty'},
           {'SELECT * FROM fact_i94 WHERE cicid IS NULL': 0},
           {'SELECT * FROM dim_time': 'not empty'},
           {'SELECT * FROM dim_time WHERE date IS NULL': 0},
           {'SELECT * FROM dim_airport': 'not empty'},
           {'SELECT * FROM dim_airport WHERE iata_code IS NULL': 0},
           {'SELECT * FROM dim_demographics': 'not empty'},
           {'SELECT * FROM dim_demographics WHERE city IS NULL': 0},
           {'SELECT * FROM dim_climate': 'not empty'},
           {'SELECT * FROM dim_climate WHERE month IS NULL': 0},
           {'SELECT * FROM dim_climate WHERE city IS NULL': 0},
           {'SELECT * FROM mapping_country': 'not empty'},
           {'SELECT * FROM mapping_country WHERE country_id IS NULL': 0}]
)


pause_cluster = RedshiftPauseClusterOperator(
    task_id='pause_cluster',
    dag=dag2,
    cluster_identifier = 'redshift-cluster-1',
    aws_conn_id = 'aws_credentials'
)


start >> [stage_demographics,
          stage_temperature,
          stage_immigration,
          stage_airport,
          stage_country]

[stage_demographics,
 stage_temperature,
 stage_immigration,
 stage_airport,
 stage_country] >> staging_done

staging_done >> [load_dim_airport, load_dim_demographics]

[load_dim_demographics, load_dim_airport] >> load_fact_i94

load_fact_i94 >> [load_dim_time, load_mapping_country]
[load_dim_time, load_mapping_country] >> load_dim_climate
load_dim_climate >> quality_check
quality_check >> [pipeline_executed, pause_cluster]
