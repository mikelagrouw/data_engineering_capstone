"""Create statements."""

class CreateStatements():
    """Staging table create statements."""

    create_staging_immigration = """
    DROP TABLE IF EXISTS staging_i94;
    CREATE TABLE IF NOT EXISTS staging_i94 (
    cicid float,
    i94yr float,
    i94mon float,
    i94cit float,
    i94res float,
    i94port varchar,
    arrdate datetime,
    i94mode float,
    i94addr varchar,
    depdate datetime,
    i94bir float,
    i94visa float,
    count float,
    dtadfile varchar,
    entdepa varchar,
    entdepd varchar,
    matflag varchar,
    biryear float,
    dtaddto varchar,
    gender varchar,
    airline varchar,
    admnum float,
    fltno varchar,
    visatype varchar
    );
    """

    create_staging_demographics = """
    DROP TABLE IF EXISTS staging_dem;
    CREATE TABLE IF NOT EXISTS staging_dem (
    City varchar,
    State varchar,
    Median_Age float,
    Male_Population float,
    Female_Population float,
    Total_Population int,
    Number_of_Veterans float,
    Foreign_born float,
    Average_Household_Size float,
    State_Code varchar
    );
    """

    create_staging_temperature = """
    DROP TABLE IF EXISTS staging_temp;
    CREATE TABLE IF NOT EXISTS staging_temp (
    dt varchar,
    City varchar,
    AverageTemperature float,
    AverageTemperatureUncertainty float,
    Country varchar,
    Latitude varchar,
    Longitude varchar
    );
    """

    create_staging_airports = """
    DROP TABLE IF EXISTS staging_air;
    CREATE TABLE IF NOT EXISTS staging_air (
    type varchar,
    name varchar,
    elevation_ft float,
    iso_region varchar,
    municipality varchar,
    iata_code varchar,
    coordinates varchar
    );
    """

    create_staging_country = """
    DROP TABLE IF EXISTS staging_country;
    CREATE TABLE IF NOT EXISTS staging_country (
    country_id float,
    country_name varchar
    );
    """


class CreateFact():
    """Fact table create statements."""

    create_fact_i94 = """
    DROP table IF EXISTS fact_i94;
    CREATE TABLE IF NOT EXISTS fact_i94 (
    cicid float PRIMARY KEY,
    i94cit float,
    i94res float,
    i94port varchar,
    arrdate datetime,
    i94mode float,
    depdate datetime,
    i94bir float,
    i94visa float,
    gender varchar,
    airline varchar
    );
    """


class CreateDim():
    """Dimension table create statements."""

    create_dim_airport = """
    DROP TABLE IF EXISTS dim_airport;
    CREATE TABLE IF NOT EXISTS dim_airport (
    type varchar,
    name varchar,
    elevation_ft float,
    municipality varchar,
    iata_code varchar
    );
    """

    create_dim_demographics = """
    DROP TABLE IF EXISTS dim_demographics;
    CREATE TABLE IF NOT EXISTS dim_demographics (
    City varchar,
    State varchar,
    Median_Age float,
    Male_Population float,
    Female_Population float,
    Total_population int,
    Number_of_Veterans float,
    Foreign_born float,
    Average_Household_Size float,
    State_Code varchar
    );
    """

    create_dim_time = """
    DROP TABLE IF EXISTS dim_time;
    CREATE TABLE IF NOT EXISTS dim_time (
    date datetime,
    year int,
    month int,
    day int,
    week int,
    dayofweek int
    );
    """

    create_dim_climate = """
    DROP TABLE IF EXISTS dim_climate;
    CREATE TABLE IF NOT EXISTS dim_climate (
    month int,
    city varchar,
    AverageTemperature float,
    AverageTemperatureUncertainty float
    )
    """

    create_mapping_country = """
    DROP TABLE IF EXISTS mapping_country;
    CREATE TABLE IF NOT EXISTS mapping_country (
    country_id float,
    country_name varchar
    );
    """
