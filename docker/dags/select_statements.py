"""Select statements."""


class SelectStatements():
    """Select statements."""

    select_fact_i94 = """
    SELECT DISTINCT
    cicid,
    i94cit,
    i94res,
    i94port,
    arrdate,
    i94mode,
    depdate,
    i94bir,
    i94visa,
    gender,
    airline
    FROM staging_i94 RIGHT JOIN dim_airport
    ON i94port = iata_code
    WHERE cicid IS NOT NULL
    """

    select_dimension_airport = """
    SELECT
    type,
    name,
    elevation_ft,
    municipality,
    iata_code
    FROM staging_air INNER JOIN staging_dem
    ON city = municipality
    """

    select_dimension_demographics = """
    (City,
    State,
    Median_Age,
    Male_Population,
    Female_Population,
    Total_population,
    Number_of_Veterans,
    Foreign_born,
    Average_Household_Size,
    State_Code) SELECT DISTINCT
    City,
    State,
    Median_Age,
    Male_Population,
    Female_Population,
    Total_population,
    Number_of_Veterans,
    Foreign_born,
    Average_Household_Size,
    State_Code
    FROM staging_dem INNER JOIN staging_air
    ON City = municipality;
    """

    select_dimension_time = """
    SELECT arrdate,
    DATE_PART(year, arrdate) as year,
    DATE_PART(month, arrdate) as month,
    DATE_PART(day, arrdate) as day,
    DATE_part(week, arrdate) as week,
    DATE_part(dow, arrdate) as day_of_week
    FROM (SELECT DISTINCT arrdate FROM staging_i94
          UNION
          SELECT DISTINCT depdate FROM staging_i94)
    WHERE arrdate IS NOT NULL
    """

    select_temperature_data = """
    select distinct
    month,
    staging_temp.city,
    averagetemperature,
    averagetemperatureuncertainty
    from staging_temp
    right join dim_demographics
    on staging_temp.city = dim_demographics.city
    right join dim_time
    on dt = month
    where Country = 'United States'
    """
    select_mapping_country = """
    select distinct country_id, country_name from staging_country
    JOIN
    (select distinct i94cit from fact_i94
    union
    select distinct i94res from fact_i94)
    on i94cit = country_id
    """
