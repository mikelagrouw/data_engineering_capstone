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


# Capstone project write up

## Step 1: Scope the Project and Gather Data

### Scope
For this project four datasets are used. the immigration,airport, temperature and us city demographics. A fact table will be created for arrivals from the immigration dataset, and several dimension tables will be included from that and the other tables. The detailed datamodel will be explained in step 3. The scope of the model will be to create a database where general insights of travel preferences. Think for instant about research question as; "Do people visiting the US from certain countries with pleasure visa tend to visit warmer states more rather than colder ones" or Do people with a bussiness visum tend to visit cities with a larger population. For this project the etl pipeline will be performed in redshift and airflow. The pipeline will be explained in more detail in step 3. In a nutshell, the datasets will be staged from s3 to redshift, the different fact and dimension tables will be filled and some quality checks will be performed.

### Describe and Gather Data
the US demographic dataset comes from OpendataSoft and consist of demographic data such as population, avg household size and mean age per city. The data also counts population by race in cities in different rows, but it is already decided in this stage of the project that I will discard those columns because I feel the categorization of race is highly uninformative and morally debatable at best.

the temperature dataset comes from kaggle and consist of temperature measurements for cities around the world on a long and fine grained timescale. this timescale does not overlap with the immigration data

The immigration data comes from the us travel and tourism office and consist of data of arrivals, such as visum type, date, arrival airport, carrier, place of departure and so on. some of this data will be included in the fact table and some will probably be stored in dimension tables. We will look into this in more debt in further steps.

the airport table consists of data of various airports over the world

## Step 2: Explore and Assess the Data

### demographics data.
Removal of race and count column, and take distinct rows
### Immigration data
remove columns with more than 50 % null values. than extract sensible date/time formats from the arrdate and depdate columns
### temperature data
group by month and city. average all other columns. this because the dates of immigration and temperature do not overlap. we transform the temperature table to a local climate table
### airport data
drop all were iata code is nan and where country is not US. than drop gps code local code, country, continent and region.


## Step 3: Define the Data Model


### Conceptualize data models
The model consists of 6 tables. main fact table is the I94 table with the immigration data. linked to the i94 table on arrival port to iata code is the airport table, with dataset from the airport. Linked to the airport data is the demographics table linked on city name in the two tables. A time table is linked to the i94 table with date column. A climate table per month per city is linked to the demographics table and the time table. a simple table with country codes in the i94 data mapped to country names is included to make more sense of some queries. see the picture of the schema for more details

### Data model
![data-model](\images\data_model.png)

### Mapping out data pipelines
all steps are executed in airflow. the database will be constructed and filled in redshift. the raw data will be saved in s3
Firstly the tables in the database have to be created. this is done with a seperate dag. Than all datasets (i94, Temperature, Airport and demographics + a csv mapping country codes to country names) have to be staged to redshift. After this firstly the airport and demographics table will be constructed with an inner join (of the airport and demographic staging tables on city name) such that no foreign key constrain is violated. After this the i94 fact table will be constructed by a right join of the i94 staging table and the airport table. after this the time table will be constructed from an union of arrdate and depdate columns in the i94 fact table. Then, the temperature column will be constructed with a right join of the temperature staging table and the time and demographics tables. Lastly the country_mapping table will be constructed by an inner join of country_stage and union of i94cit and i94res from the i94 table.

## Step 4: Run Pipelines to Model the Data

### Run data pipelines

see below for airflow dag
![setup](\images\setup_dag.png)
dag used for creating connections and tables in redsift
![etl](\images\etl_dag.png)
dag used for staging, loading and quality checks
### Run quality checks
Two quality checks are conducted for every fact and dimension table.To check if the tables contain data, all data in the table is selected and checked whether there are more than 0 rows in the data. To check if their are no null values in the primary keys, the primary key column is selected where primary key is null. the quality checker checks whether this selection contains zero rows as expected.

## Step 5: Complete Project Write Up
Data assesment and preprocessing was done with pandas, with this amount of data still feasible and easy to use.
S3 was used to store the raw data because its easily accessible with redshift and can contain large amounts of data
Redshift was used for all the etl processess  

### Data Dictionary

#### Fact_i94
|Column|Data type|Description |
|------|------|---------|
|cicid|int| Primary key: id of the arrival.|
i94cit|float| Code of country from which the person travels.
i94res|float| Code of country code of residence.
i94port|varchar| Airport of arrival, foreign key to the airport table.
arrdate|datetime| Date of arrival, foreign key to the date table
i94mode|int| mode of transportation; 1: air, 2: sea, 3: land, 9: not reported.
depdate|datetime| date of departure, foreign key to the airport table.
i94bir|float| Age of respondent.
i94visa|float| Visatype; 1: bussiness, 2: pleasure, 3: student
gender|varchar| Gender of respondent
airline|varchar| airline travelled with

#### Dimension time
|Column| Data type| Description|
|-----|------|------|
date| datetime| Primary key: date
year |int| Year
month |int| Month|
day |int| Day of the month
week |int| Week of the year
dayofweek |int| Day of the week, starting from 0 on sunday

#### Dimension airport
|Column| Data type| Description|
|-----|------|------|
type| varchar | Type of airport
name| varchar | Name of airport
elevation_ft| float | Elevation in feet of airport
municipality| varchar | City of airport, foreign key to demographics table
iata_code| varchar | Primary key: iata code

#### Dimension demographics
|Column| Data type| Description|
|-----|------|------|
City| varchar| Primary key: city
State| varchar| State
Median_Age| float| Median age in city
Male_Population| float| Male polulation in city
Female_Population| float| Female population in city
Total_population| int| Total population of city
Number_of_Veterans| float| Number of veterans in city
Foreign_born| float| Amount of foreign born people in city
Average_Household_Size| float| Average household size in city
State_Code| varchar| State code of the state of city

#### Dimension temperature
|Column| Data type| Description|
|-----|------|------|
month| int| Primary key: month
city| varchar| Primary key month, foreign key to demographics table
AverageTemperature| float| Average temperature per month and city
AverageTemperatureUncertainty| float| Average temperature uncertainty per month and city

#### Country mapping table
|Column| Data type| Description|
|-----|------|------|
country_id| float| Primary key: id
country_name| varchar| country name

### Finishing remarks
I propose the data should be updated daily, as this is the finest timescale of the i94 data.
If the data was increased by a 100-fold, more nodes should be aquired in redshift to run the etl. the data preprocessing should be conducted with spark and emdr instead of pandas locally.
If the data should be run on daily basis on 7 am this should be scheduled in airflow.
if the database should be accesible by 100+ people, these people should all get their own iam user with specific rights to read the redshift database.
