#%%
from pyspark.sql import SparkSession
import preprocess as pre
import time
import config

csv_path = config.SMALL_CSV
parquet_path = config.SMALL_PARQUET
hourly_path = config.SMALL_COMPUTED + "/hourly"
daily_path = config.SMALL_COMPUTED + "/daily"

# Initialize Spark Session and read data
spark = SparkSession.builder.master(config.PRE_SPARK_MASTER)\
    .config("spark.driver.memory", config.DRIVER_MEMORY) \
    .config("spark.executor.memory", config.EXECUTOR_MEMORY) \
    .appName("Weather OLAP Preprocessing").getOrCreate()

try:
    csv_df = spark.read.csv(csv_path, header=True)
    print("Small set of CSV Data read.")
except:
    print("Not CSV files")
    exit(0)

print("Total rows: ", round(csv_df.count()))

# ======
# Use CSV files directly for processing
# ======
print("CSV files processing time -----------")
total_time = 0
start = time.time()
hourly_df, location_df = pre.process_hourly_locs(spark, csv_df)
hourly_count = hourly_df.count()
count = location_df.count()
end = time.time()
total_time += end - start
print(f"Hourly table and location table takes {end - start:.2f}s with hourly table rows = {hourly_count} and {count} station")

# ======
# Conversion to Parquet and using Parquet file for processing
# ======
print("\nParquet files convertion and processing time -----------")
total_time = 0

start = time.time()
csv_df.write.parquet(parquet_path, mode="overwrite")
parquet_df = spark.read.parquet(parquet_path)
end = time.time()
total_time += end - start
print(f"Directly converting to parquet files and reading it back takes {end - start:.2f}s")

start = time.time()
hourly_df, location_df = pre.process_hourly_locs(spark, parquet_df)
hourly_count = hourly_df.count()
count = location_df.count()
end = time.time()
total_time += end - start
print(f"Hourly table and location table takes {end - start:.2f}s with hourly table rows = {hourly_count} and {count} station")
print(f"Total time take {total_time:.2f}s", end = "\n")

# Save hourly table and read the hourly table
hourly_df.write.parquet(hourly_path, mode="overwrite", partitionBy=["year", "month"])

# ======
# The rest precomputed tables regardless the source data format
# ======
# using previously saved hourly table
hourly_df = spark.read.parquet(hourly_path)

# Daily table
total_time = 0
start = time.time()
daily_df = pre.process_daily(hourly_df)
count = daily_df.count()
end = time.time()
total_time += end - start
print(f"Daily table takes {end - start:.2f}s with rows = {count}")

# Save daily table and read the daily table
daily_df.write.parquet(daily_path, mode="overwrite", partitionBy=["year", "month"])
daily_df = spark.read.parquet(daily_path)

# Monthly
start = time.time()
monthly_df = pre.process_monthly(daily_df)
count = monthly_df.count()
end = time.time()
total_time += end - start
print(f"Monthly table takes {end - start:.2f}s with rows = {count}")

# Yearly
start = time.time()
yearly_df = pre.process_yearly(daily_df)
count = yearly_df.count()
end = time.time()
total_time += end - start
print(f"Yearly table takes {end - start:.2f}s with rows = {count}")
print(f"Total time for processing the daily, monthly, and yearly table (exluding writing time) is {total_time:.2f}s")

# wait to keep the spark session for viewing Spark Job history
x = input("Done! Press Enter with any key to terminate") 
spark.stop()