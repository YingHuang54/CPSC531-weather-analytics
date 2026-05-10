
#%%
from pyspark.sql import SparkSession
import config

data_path = config.RAW_PATH
target_path = config.PARQUET_PATH

# Initialize Spark Session and read data
spark = SparkSession.builder.master(config.PRE_SPARK_MASTER)\
    .config("spark.driver.memory", config.DRIVER_MEMORY) \
    .config("spark.executor.memory", config.EXECUTOR_MEMORY) \
    .appName("Weather OLAP Preprocessing").getOrCreate()
source_df = spark.read.csv(data_path, 
                           header=True, 
                           timestampFormat="yyyy-MM-dd HH:mm:ss")
source_df.show(5)

# Write as parquet
source_df.write.parquet(target_path, mode="overwrite")

spark.stop()
