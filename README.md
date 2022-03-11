# README
detail about project can be found in write_up.md
## files
all functional code can be found in the Capstone_Project_Template.ipynb (for data preprcessing), and in the docker/dag/ folder. in the dag folder the dags are run by set_up_dag.py and etl_pipeline.py. helper functions are found in load_table.py s3toredshift.py and quality_operator.py. sql statements can be found in select_statements.py and create_statements.py

## data
data files used in this project are to big to uplead to github, temporarily they are stored in an s3 bucket named capstonemike
## set up
To run on your own machine, use the docker-compose file included to create an airflow environment. After starting airflow, the set up dag has to run first. The set up dag created connections to redshift and aws in airflow if not yet specified. To run locally specify your own aws and redshift credentials in the configuration.py file found in the dag folder.
to run airflow with the docker file run

docker-compose up airflow-init

and

docker-compose up

in the local command line in the docker folder.

you can now enter airflow on localhost 8080

DAGS used for the project are a_set_up_dag and etl_pipeline
