# Provide the code to query different precomputed tables 
# and generate JSON representation of their bar graph visualization.

from pyspark.sql import SparkSession
from datetime import datetime
import pandas as pd
import plotly.express as pex
import plotly.io as pio

date_str_format = "%Y-%m-%d"
DF_SIZE_LIMIT = 380000 # hard limit for the number of rows allow to convert to pandas for generting map

# generate the graph from pandas 
def generate_graph(pdf, x : str, y : str):
    """Generate bar graph object from the pandas dataframe on the specified x column and y column
    and return its JSON string representation
    """
    fig = pex.bar(data_frame=pdf, x = x, y = y
                   , color = y, 
                  color_continuous_scale="Viridis_r",
                  range_color=[pdf[y].min(), pdf[y].max()]
                )
    fig.update_layout(bargap = 0)
    return pio.to_json(fig)

# for daily level graph
def daily(spark : SparkSession, daily_table, params):
    """ Generate daily graph based on the parameters in params.
    """
    start_date = datetime.strptime(params["start_date"], date_str_format)
    end_date = datetime.strptime(params["end_date"], date_str_format)
    time_range_sql = ''
    result_col =params["agg"] + params["metric"]
    
    # Write the part of SQL where clause to select data between start_date and end_date
    if start_date.year != end_date.year:
        time_range_sql = f'''(
        (year > {start_date.year} AND year < {end_date.year})
        OR (year = {start_date.year} AND month > {start_date.month}) 
        OR (year = {end_date.year} AND month < {end_date.month})
        OR (
        (year = {start_date.year} AND month = {start_date.month} AND `DATE` >= to_date('{params["start_date"]}')) 
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

    # Complete SQL Query
    filter_sql = f''' 
            SELECT 
            `DATE`,
            {result_col}
            FROM {daily_table} AS m
            WHERE {time_range_sql}
            AND id = '{params["id"]}'
    '''

    filtered_df = spark.sql(filter_sql)

    # Error check, convert, sort, and graph
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
    pdf = None
    try:
        pdf = filtered_df.toPandas()
    except:
        raise Exception("Spark Dataframe Convert To Pandas Failed, " \
        "please try again with smaller time period.")
    
    return generate_graph(pdf, 'DATE', result_col)

# for montly level graph
def monthly(spark, monthly_table, params):
    """ Generate monthly graph based on the parameters in params.
    To maintain valid comparison, the whole month is considered, 
    which means the days in the start_date and end_date in the params are ignored.
    """
    start_date = datetime.strptime(params["start_date"], date_str_format)
    end_date = datetime.strptime(params["end_date"], date_str_format)
    result_col =params["agg"] + params["metric"]

    # Write the part of SQL where clause to select data between start_date and end_date
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

    # Complete SQL Query
    filter_sql = f''' 
        SELECT 
        year*100+month AS year_month,
        {result_col}
        FROM {monthly_table} AS m
        WHERE 
        {time_range_sql}
        AND id = '{params["id"]}'
        '''
    filtered_df = spark.sql(filter_sql)

    # Error check, convert, sort, and graph
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
    pdf = None
    try:
        pdf = filtered_df.toPandas()
    except:
        raise Exception("Spark Dataframe Convert To Pandas Failed, " \
        "please try again with smaller time period.")
    transformed_pdf = pdf.sort_values("year_month", ascending=True)
    transformed_pdf["year_month"] = pd.to_datetime(transformed_pdf["year_month"],format="%Y%m").dt.strftime("%Y %b")

    return generate_graph(transformed_pdf, "year_month", result_col)

# for yearly level graph
def yearly(spark, yearly_table, params):
    """ Generate yearly graph based on the parameters in params.
    To maintain valid comparison, the whole year is considered, 
    which means the days and months in the start_date and end_date in the params are ignored.
    """
    start_date = datetime.strptime(params["start_date"], date_str_format)
    end_date = datetime.strptime(params["end_date"], date_str_format)
    time_range_sql = ''
    result_col =params["agg"] + params["metric"]

    # Write the part of SQL where clause to select data between start_date and end_date
    time_range_sql = f'''(
        year >= {start_date.year} AND year <= {end_date.year}
        )'''
    
    # Complete SQL Query
    filter_sql = f''' 
        SELECT 
        year,
        {result_col}
        FROM {yearly_table} AS m
        WHERE 
        {time_range_sql}
        AND id = '{params["id"]}'
        '''
    filtered_df = spark.sql(filter_sql)

    # Error check, convert, sort, and graph
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
    pdf = None
    try:
        pdf = filtered_df.toPandas()
    except:
        raise Exception("Spark Dataframe Convert To Pandas Failed, " \
        "please try again with smaller time period.")
    
    return generate_graph(pdf, "year", result_col)

# for hourly level graph
def hourly(spark, hourly_table, params):
    """ Generate hourly graph based on the parameters in params.
    """
    result_col = params["metric"]
    
    # Write the part of SQL where clause to select data between start_date and end_date
    start_date = datetime.strptime(params["start_date"], date_str_format)
    end_date = datetime.strptime(params["end_date"], date_str_format)
    time_range_sql = ''
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

    # Complete SQL Query
    filter_sql = f''' 
            SELECT 
            `DATE`,
            {result_col}
            FROM {hourly_table} AS m
            WHERE {time_range_sql}
            AND id = '{params["id"]}'
    '''

    filtered_df = spark.sql(filter_sql)

    # Error check, convert, sort, and graph
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
    pdf = None
    try:
        pdf = filtered_df.toPandas()
    except:
        raise Exception("Spark Dataframe Convert To Pandas Failed, " \
        "please try again with smaller time period.")
    
    return generate_graph(pdf, 'DATE', result_col)