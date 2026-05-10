# Script to compare the Visualization performance of the system when 
# the precomputed tables are repartition before written or not repartioned.
import time
from analytics import create_graph
from analytics import create_map
from flask_app import data_loader
import config
import plotly.graph_objects as go
date_str_format = "%Y-%m-%d"

# start the spark session
spark = data_loader.spark # same spark session configuration as the Flask App
run_number = 5

# The dictionary for Spark SQL temporary view name
view_dict = {
    'hourly' : 'hourly_view',
    'daily' : 'daily_view',
    'monthly' : 'monthly_view',
    'yearly' : 'yearly_view',
    'location' : 'location_table'
}

def measure_time_graph(fn, spark, params):
    """Measure the time for graph generation from precomputed tables"""
    # warm up
    fn(spark, view_dict[params["level"]], params)
    fn(spark, view_dict[params["level"]], params)

    # measure the time without cache
    total_time = 0
    for _ in range(run_number):
        spark.catalog.clearCache()
        start = time.time()
        fn(spark, view_dict[params["level"]], params)
        end = time.time()
        total_time += end - start
    return round(total_time/run_number, 2)

def measure_time_map(fn, spark, params):
    """Measure the time for graph generation from precomputed tables"""
    # warm up
    fn(spark, view_dict[params["level"]], view_dict["location"], params)
    fn(spark, view_dict[params["level"]], view_dict["location"], params)

    # measure the time without cache
    total_time = 0
    for _ in range(run_number):
        spark.catalog.clearCache()
        start = time.time()
        fn(spark, view_dict[params["level"]], view_dict["location"], params)
        end = time.time()
        total_time += end - start
    return round(total_time/run_number, 2)

# 24 hours
hourly_params = {
    "start_date" : "2022-12-30", #
    "end_date" : "2022-12-30", #
    "id" : "12300-6213", # None or id in format "12300-6213"
    "level" : "hourly", # day,month,year
    "agg" : "",
    "metric" : "TMP" # 
}
# 24 days
daily_params = {
    "start_date" : "2020-01-01", #
    "end_date" : "2020-01-24", #
    "id" : "12300-6213", # None or id in format "12300-6213"
    "level" : "daily", # day,month,year
    "agg" : "avg",
    "metric" : "TMP" # 
}
# 24 months
monthly_params = {
    "start_date" : "2014-01-01", #
    "end_date" : "2015-12-31", #
    "id" : "12300-6213", # None or id in format "12300-6213"
    "level" : "monthly", # day,month,year
    "agg" : "avg",
    "metric" : "TMP" # 
}
# 6 year
yearly_params = {
    "start_date" : "2014-01-01", #
    "end_date" : "2019-12-31", #
    "id" : "12300-6213", # None or id in format "12300-6213"
    "level" : "yearly", # day,month,year
    "agg" : "avg",
    "metric" : "TMP" # 
}

#%% for repartitioned
print("Testing visualization of repartitioned data")
target_path = config.LARGER_PATH
hourly_path = target_path + "/hourly"
daily_path = target_path + "/daily"
location_path = target_path + "/locations"
monthly_path = target_path + "/monthly"
yearly_path = target_path + "/yearly"

hourly_df = spark.read.parquet(hourly_path)
hourly_df.createOrReplaceTempView(view_dict['hourly'])

daily_df = spark.read.parquet(daily_path)
daily_df.createOrReplaceTempView(view_dict['daily'])

monthly_df = spark.read.parquet(monthly_path)
monthly_df.createOrReplaceTempView(view_dict['monthly'])

yearly_df = spark.read.parquet(yearly_path)
yearly_df.createOrReplaceTempView(view_dict['yearly'])

location_df = spark.read.parquet(location_path)
location_df.createOrReplaceTempView(view_dict['location'])

# Graph time
hist_part = []
print(f"Hourly Graph Time for 1 day: ", end = "")
result = measure_time_graph(create_graph.hourly, spark, hourly_params)
hist_part.append(result)
print(f"{result:.2f} s")

print(f"Daily Graph Time for 24 days: ")
result = measure_time_graph(create_graph.daily, spark, daily_params)
hist_part.append(result)
print(f"{result:.2f} s")

print(f"Monthly Graph Time for 24 months: ", end = "")
result = measure_time_graph(create_graph.monthly, spark, monthly_params)
hist_part.append(result)
print(f"{result:.2f} s")


print(f"Yearly Graph Time for 6 years ", end = "")
result = measure_time_graph(create_graph.yearly, spark, yearly_params)
hist_part.append(result)
print(f"{result:.2f} s")

# Map time
mhist_part = []
print(f"Hourly Map Time for 1 day: ", end = "")
result = measure_time_map(create_map.hourly, spark, hourly_params)
mhist_part.append(result)
print(f"{result:.2f} s")

print(f"Daily Map Time for 24 days: ")
result = measure_time_map(create_map.daily, spark, daily_params)
mhist_part.append(result)
print(f"{result:.2f} s")

print(f"Monthly Map Time for 24 months: ", end = "")
result = measure_time_map(create_map.monthly, spark, monthly_params)
mhist_part.append(result)
print(f"{result:.2f} s")


print(f"Yearly Map Time for 6 years ", end = "")
result = measure_time_map(create_map.yearly, spark, yearly_params)
mhist_part.append(result)
print(f"{result:.2f} s")



#%% for tiny files
print("Testing visualization of data stored in tiny files")
target_path = config.TINY_PATH
hourly_path = target_path + "/hourly"
daily_path = target_path + "/daily"
location_path = target_path + "/locations"
monthly_path = target_path + "/monthly"
yearly_path = target_path + "/yearly"

hourly_df = spark.read.parquet(hourly_path)
hourly_df.createOrReplaceTempView(view_dict['hourly'])

daily_df = spark.read.parquet(daily_path)
daily_df.createOrReplaceTempView(view_dict['daily'])

monthly_df = spark.read.parquet(monthly_path)
monthly_df.createOrReplaceTempView(view_dict['monthly'])

yearly_df = spark.read.parquet(yearly_path)
yearly_df.createOrReplaceTempView(view_dict['yearly'])

location_df = spark.read.parquet(location_path)
location_df.createOrReplaceTempView(view_dict['location'])

# graph time result
hist_tiny = []
print(f"Hourly Graph Time for 1 day: ", end = "")
result = measure_time_graph(create_graph.hourly, spark, hourly_params)
hist_tiny.append(result)
print(f"{result:.2f} s")

print(f"Daily Graph Time for 6 days: ")
result = measure_time_graph(create_graph.daily, spark, daily_params)
hist_tiny.append(result)
print(f"{result:.2f} s")

print(f"Monthly Graph Time for 6 months: ", end = "")
result = measure_time_graph(create_graph.monthly, spark, monthly_params)
hist_tiny.append(result)
print(f"{result:.2f} s")

print(f"Yearly Graph Time for 6 years ", end = "")
result = measure_time_graph(create_graph.yearly, spark, yearly_params)
hist_tiny.append(result)
print(f"{result:.2f} s")

# Map time
mhist_tiny = []
print(f"Hourly Map Time for 1 day: ", end = "")
result = measure_time_map(create_map.hourly, spark, hourly_params)
mhist_tiny.append(result)
print(f"{result:.2f} s")

print(f"Daily Map Time for 6 days: ")
result = measure_time_map(create_map.daily, spark, daily_params)
mhist_tiny.append(result)
print(f"{result:.2f} s")

print(f"Monthly Map Time for 6 months: ", end = "")
result = measure_time_map(create_map.monthly, spark, monthly_params)
mhist_tiny.append(result)
print(f"{result:.2f} s")


print(f"Yearly Map Time for 6 years ", end = "")
result = measure_time_map(create_map.yearly, spark, yearly_params)
mhist_tiny.append(result)
print(f"{result:.2f} s")

#%% Plot the result
levels = ["Hourly (24 hours)", "Daily (24 days)", "Monthly (24 months)", "Yearly (6 years)"]
fig = go.Figure(data = [
    go.Bar(name="Larger Partition", x=levels, y=hist_part, text=hist_part),
    go.Bar(name="Smaller Partition", x=levels, y=hist_tiny, text=hist_tiny)
])
fig.update_layout(
    title = "Before rearition vs Small Partitions for Graph Visualization",
    xaxis_title = "Level (Time range)",
    yaxis_title = f"Average Query and Visualization Time In {run_number} Runs (second)",
    barmode = "group",
)
f = open("graph_test_result.html", "w")
fig.write_html(f)

fig = go.Figure(data = [
    go.Bar(name="Larger Partition", x=levels, y=mhist_part, text=mhist_part),
    go.Bar(name="Small Partition", x=levels, y=mhist_tiny, text=mhist_tiny)
])
fig.update_layout(
    title = "Larger Parition vs Small Partitions for Map Visualization",
    xaxis_title = "Level (Time range)",
    yaxis_title = f"Average Query and Visualization Time In {run_number} Runs (second)",
    barmode = "group",
)
f = open("map_test_result.html", "w")
fig.write_html(f)
# %%
