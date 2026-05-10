from pyspark.sql import SparkSession
import config

# start the spark session
spark = SparkSession.builder.master(config.SPARK_MASTER)\
    .config("spark.driver.memory", config.DRIVER_MEMORY) \
    .config("spark.executor.memory", config.EXECUTOR_MEMORY) \
    .appName("Weather OLAP").getOrCreate()

# The dictionary for Spark SQL temporary view name
view_dict = {
    'hourly' : 'hourly_view',
    'daily' : 'daily_view',
    'monthly' : 'monthly_view',
    'yearly' : 'yearly_view',
    'location' : 'location_table'
}

# prepare the Spark Session and Spark SQL view for backend data query
def load_data():
    target_path = config.PRECOMPUTED_PATH
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