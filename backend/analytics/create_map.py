# Provide the code to query different precomputed tables 
# and generate JSON representation of their scatter map visualization.

import pandas as pd
import plotly.express as pex
import plotly.io as pio
from datetime import datetime

date_str_format = "%Y-%m-%d"
DF_SIZE_LIMIT = 380000 # hard limit for the number of rows allow to convert to pandas for generting map

def pandas_to_map(pdf, result_col, animation_col):
    """Generate scatter map object from the pandas dataframe on the specify x column and y column
    and return its JSON string representation.
    """
    fig = pex.scatter_geo(data_frame=pdf,lat="LATITUDE",lon="LONGITUDE",
                    hover_name= "city",
                    hover_data=[result_col, "id","LATITUDE","LONGITUDE"],
                    color = result_col,
                    range_color=[pdf[result_col].min(), pdf[result_col].max()],
                    color_continuous_scale= "spectral_r",
                    animation_frame=animation_col)
    fig.update_geos(showcountries = True)
    return pio.to_json(fig)

def hourly(spark, hourly_table : str, location_table, params):
    """ Generate hourly map based on the parameters in params """

    start_date = datetime.strptime(params["start_date"], date_str_format)
    end_date = datetime.strptime(params["end_date"], date_str_format)
    result_col = params["metric"]
    time_range_sql = ""

    # Write the part of SQL where clause to select data between start_date and end_date
    if start_date.year != end_date.year:
        time_range_sql = f'''(
        (year > {start_date.year} AND year < {end_date.year})
        OR (year = {start_date.year} AND month > {start_date.month}) 
        OR (year = {end_date.year} AND month < {end_date.month})
        OR (
            (year = {start_date.year} AND month = {start_date.month} AND `DATE` >= to_date('{params["start_date"]}) 
            AND (year = {end_date.year} AND month = {end_date.month} AND `DATE` <= to_date('{params["end_date"]}'))
        )
        )'''
    else:
        time_range_sql = f'''(
        (year = {start_date.year})
        AND (
            (month > {start_date.month} AND month < {end_date.month}) 
            OR (
                (month = {start_date.month} AND `DATE` >= to_date('{params["start_date"]}'))
                AND (month = {end_date.month} AND `DATE` <= to_date('{params["end_date"]}'))
            )
        )
        )'''
    
    # complete sql command
    filter_sql = f'''
        WITH filtered AS (
        SELECT 
        id,
        `DATE`,
        year,
        month,
        ROUND({result_col}, 2) AS {result_col}
        FROM {hourly_table} AS m
        WHERE {time_range_sql}
        )
        SELECT 
        filtered.DATE,
        filtered.id,
        filtered.{result_col},
        {location_table}.city,
        {location_table}.LATITUDE,
        {location_table}.LONGITUDE
        FROM filtered
        LEFT JOIN {location_table}
        ON filtered.id = {location_table}.id
        '''
    filtered_df = spark.sql(filter_sql)

    # Error check, convert, sort, and map
    if filtered_df.count() == 0:
        return {
            'error': "No Data",
            "message": "No Data, try a different input."
        }
    elif filtered_df.count() > DF_SIZE_LIMIT:
        print("Too large ", filtered_df.count(), flush=True)
        return {
            'error': "Too Large",
            "message": "Data too large, try a different time range"
        }
    try:
        pdf = filtered_df.toPandas()
    except:
        raise Exception("Spark Dataframe Convert To Pandas Failed, " \
        "please try again with smaller time period.")
    transformed_pdf = pdf.sort_values("DATE", ascending=True)
    return pandas_to_map(transformed_pdf, result_col, "DATE")

def daily(spark, daily_table : str, location_table, params):
    """ Generate daily map based on the parameters in params """
    date_str_format = "%Y-%m-%d"
    start_date = datetime.strptime(params["start_date"], date_str_format)
    end_date = datetime.strptime(params["end_date"], date_str_format)
    result_col = params["agg"] + params["metric"]

    # Write the part of SQL where clause to select data between start_date and end_date
    time_range_sql = ""
    if start_date.year != end_date.year:
        time_range_sql = f'''(
        (year > {start_date.year} AND year < {end_date.year})
        OR (year = {start_date.year} AND month > {start_date.month}) 
        OR (year = {end_date.year} AND month < {end_date.month})
        OR (
            (year = {start_date.year} AND month = {start_date.month} AND `DATE` >= to_date('{params["start_date"]}) 
            AND (year = {end_date.year} AND month = {end_date.month} AND `DATE` <= to_date('{params["end_date"]}'))
        )
        )'''
    else:
        time_range_sql = f'''(
        (year = {start_date.year})
        AND (
            (month > {start_date.month} AND month < {end_date.month}) 
            OR (
                (month = {start_date.month} AND `DATE` >= to_date('{params["start_date"]}'))
                AND (month = {end_date.month} AND `DATE` <= to_date('{params["end_date"]}'))
            )
        )
        )'''

    # The full sql command
    filter_sql = f'''
        WITH filtered AS (
        SELECT 
        id,
        `DATE`,
        {result_col}
        FROM {daily_table} AS m
        WHERE {time_range_sql}
        )
        SELECT 
        filtered.DATE,
        filtered.id,
        filtered.{result_col},
        {location_table}.city,
        {location_table}.LATITUDE,
        {location_table}.LONGITUDE
        FROM filtered
        LEFT JOIN {location_table}
        ON filtered.id = {location_table}.id
        '''
    filtered_df = spark.sql(filter_sql)

    # Error check, convert, sort, and map
    if filtered_df.count() == 0:
        return {
            'error': "No Data",
            "message": "No Data, try a different input."
        }
    elif filtered_df.count() > DF_SIZE_LIMIT:
        print("Too large ", filtered_df.count(), flush=True)
        return {
            'error': "Too Large",
            "message": "Data too large, try a different time range"
        }
    try:
        pdf = filtered_df.drop('year').drop('month').toPandas()
    except:
        raise Exception("Spark Dataframe Convert To Pandas Failed, " \
        "please try again with smaller time period.")
    transformed_pdf = pdf.sort_values("DATE", ascending=True)
    return pandas_to_map(transformed_pdf, result_col, "DATE")

def monthly(spark, monthly_table, location_table, params):
    """ Generate monthly map based on the parameters in params.
    To maintain valid comparison, the whole month is considered, 
    which means the days in the start_date and end_date in the params are ignored.
    """
    result_col = params["agg"] + params["metric"]

    # Write the part of SQL where clause to select data between start_date and end_date
    start_date = datetime.strptime(params["start_date"], date_str_format)
    end_date = datetime.strptime(params["end_date"], date_str_format)
    time_range_sql = ''
    if start_date.year != end_date.year:
        time_range_sql = f'''(
        (year > {start_date.year} AND year < {end_date.year})
        OR (year = {start_date.year} AND month >= {start_date.month}) 
        OR (year = {end_date.year} AND month <= {end_date.month})
        )'''
    else:
        time_range_sql = f'''
        (year = {start_date.year})
        AND (month >= {start_date.month} AND month <= {end_date.month}) 
        '''

    # complete sql command
    filter_sql = f'''
        WITH filtered AS (SELECT 
        year,
        month,
        year*100+month AS year_month,
        id,
        {result_col}
        FROM {monthly_table} AS m
        WHERE 
        {time_range_sql})
        SELECT 
        filtered.year_month,
        filtered.id,
        filtered.{result_col},
        {location_table}.city,
        {location_table}.LATITUDE,
        {location_table}.LONGITUDE
        FROM filtered
        LEFT JOIN {location_table}
        ON filtered.id = {location_table}.id
        '''
    filtered_df = spark.sql(filter_sql)

    # Error check, convert, sort, and map
    if filtered_df.count() == 0:
        return {
            'error': "No Data",
            "message": "No Data, try a different input."
        }
    elif filtered_df.count() > DF_SIZE_LIMIT:
        print("Too large ", filtered_df.count(), flush=True)
        return {
            'error': "Too Large",
            "message": "Data too large, try a different time range"
        }
    pdf = filtered_df.toPandas()
    animation_col = "year_month"
    transformed_pdf = pdf.sort_values(animation_col, ascending=True)
    transformed_pdf[animation_col] = pd.to_datetime(transformed_pdf[animation_col], 
                                        format="%Y%m").dt.strftime("%Y %b")
    return pandas_to_map(transformed_pdf, result_col, animation_col)
    
def yearly(spark, yearly_table, location_table, params):
    """ Generate yearly map based on the parameters in params.
    To maintain valid comparison, the whole year is considered, 
    which means the days and months in the start_date and end_date in the params are ignored.
    """
    result_col = f'''{params["agg"]}{params["metric"]}'''

    # Write the part of sql squery in where clause to select data between start_date and end_date
    start_date = datetime.strptime(params["start_date"], date_str_format)
    end_date = datetime.strptime(params["end_date"], date_str_format)
    time_range_sql = f'''(
        year >= {start_date.year} AND year <= {end_date.year}
        )'''

    # Complete SQL Query
    filter_sql = f'''
        WITH filtered AS (SELECT 
        year,
        id,
        {result_col}
        FROM {yearly_table} AS m
        WHERE 
        {time_range_sql})
        SELECT 
        filtered.year,
        filtered.id,
        filtered.{result_col},
        {location_table}.city,
        {location_table}.LATITUDE,
        {location_table}.LONGITUDE
        FROM filtered
        LEFT JOIN {location_table}
        ON filtered.id = {location_table}.id
        '''
    filtered_df = spark.sql(filter_sql)

    # Error check, convert, sort, and map
    if filtered_df.count() == 0:
        return {
            'error': "No Data",
            "message": "No Data, try a different input."
        }
    elif filtered_df.count() > DF_SIZE_LIMIT:
        print("Too large ", filtered_df.count(), flush=True)
        return {
            'error': "Too Large",
            "message": "Data too large, try a different time range"
        }
    pdf = filtered_df.toPandas()
    transformed_pdf = pdf.sort_values("year", ascending=True)
    return pandas_to_map(transformed_pdf, result_col, "year")

