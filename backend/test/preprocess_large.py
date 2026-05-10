# Script of the preprocessing stage for preprocess_analysis
# Use repartition before writing all precomputed tables (except Location Table)

# =======
# Initialization
# =======
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
import config

data_path = config.PARQUET_PATH
target_path = config.PRECOMPUTED_PATH
hourly_path = target_path + "/hourly"
daily_path = target_path + "/daily"
location_path = target_path + "/locations"
monthly_path = target_path + "/monthly"
yearly_path = target_path + "/yearly"
location_map_path = config.LOCATION_MAP_PATH

# Initialize Spark Session and read data
spark = SparkSession.builder.master(config.PRE_SPARK_MASTER)\
    .config("spark.driver.memory", config.DRIVER_MEMORY) \
    .config("spark.executor.memory", config.EXECUTOR_MEMORY) \
    .appName("Weather OLAP Preprocessing").getOrCreate()

# ======= 
# Generate Dataframe for Each Precomputed Tables
# =======
def process_hourly_locs(spark, source_df):
    "Generate hourly tables for the metrics and the separate fact table for each weather station coordinates"
    # round the coordinates
    hourly_df = source_df.withColumns({
                        "LONGITUDE" : sf.round("LONGITUDE", 2), 
                        "LATITUDE" : sf.round("LATITUDE", 2)
                        })
    # assign unique id for each weather station coordinates
    hourly_df = hourly_df.withColumn("id", sf.concat_ws("-", 100*(hourly_df.LATITUDE + 90).cast("int"), (100*(hourly_df.LONGITUDE + 180)).cast("int")))
    # rename the wind rate for better consistency with other metric
    hourly_df = hourly_df.withColumnRenamed("WND_RATE", "WIND")
    # separate the year and month for later partitioning
    hourly_df = hourly_df.withColumns({"year" : sf.year("DATE"), "month": sf.month("DATE")})
    
    #----Location table ----
    import reverse_geocoder as rg   # package for finding the closest city using coordinates
    print("Generating Location Table...")
    # unique coordinates
    location_df = hourly_df.select("id", "LONGITUDE", "LATITUDE").distinct()

    # Generate the city column for the location table
    def find_city(locations):
        """Find the city "city, country code" for each wheather stations using their coordinates (latitude, longitude)
        Return the list of matching coordinates and cities. """
        locations = list(locations)
        coords = [(location["LATITUDE"], location["LONGITUDE"]) for location in locations]
        results = rg.search(coords)
        output = []
        for (latitude, longitude), result in zip(coords, results):
            city = result["name"] + ", " + result["cc"]
            output.append((latitude, longitude, city ))
        return output
    rdd = location_df.rdd.mapPartitions(find_city)
    city_df = spark.createDataFrame(rdd, ("LATITUDE", "LONGITUDE", "city"))
    # the id column for each station
    city_df = city_df.withColumn("id", sf.concat_ws("-", 100*(city_df.LATITUDE + 90).cast("int"), (100*(city_df.LONGITUDE + 180)).cast("int"))).orderBy("id")

    # no longer need the coordinates in the precomputed metric table
    hourly_df = hourly_df.drop("LONGITUDE").drop("LATITUDE")
    hourly_df = hourly_df.repartition("year", "month")
    return hourly_df, city_df
    
def process_daily(hourly_df):
    """Generate the daily table using hourly table"""

    # no used columns
    hourly_df = hourly_df.drop("MASK")
    hourly_df = hourly_df.drop("TIME_DIFF")

    # make sure the date is in timestamp format
    hourly_df = hourly_df.withColumns({"DATE": sf.to_date("DATE")})
    
    #----- Compute daily metrics ------
    print("Computing Daily Metrics")
    # temperature aggregations
    temp_df = hourly_df.select(["id", "DATE", "year", "month", "TMP"])
    computed_df = temp_df.groupBy(["id", "DATE", "year", "month"])\
        .agg(sf.round(sf.avg("TMP"), 2).alias("avgTMP"),
            sf.round(sf.min("TMP"), 2).alias("minTMP"),
            sf.round(sf.max("TMP"), 2).alias("maxTMP"))

    # Dew Point aggregations
    temp_df = hourly_df.select(["id", "DATE", "year", "month", "DEW"])
    temp2_df = temp_df.groupBy(["id", "DATE", "year", "month"])\
        .agg(sf.round(sf.avg("DEW"), 2).alias("avgDEW"),
            sf.round(sf.min("DEW"), 2).alias("minDEW"),
            sf.round(sf.max("DEW"), 2).alias("maxDEW"))
    computed_df = computed_df.join(temp2_df, ["id", "DATE", "year", "month"])

    # Wind Rate aggregations
    temp_df = hourly_df.select(["id", "DATE", "year", "month", "WIND"])
    temp2_df = temp_df.groupBy(["id", "DATE", "year", "month"])\
        .agg(sf.round(sf.avg("WIND"), 2).alias("avgWIND"),
            sf.round(sf.min("WIND"), 2).alias("minWIND"),
            sf.round(sf.max("WIND"), 2).alias("maxWIND"))
    computed_df = computed_df.join(temp2_df, ["id", "DATE", "year", "month"])

    # Sea Level Pressure aggregations
    temp_df = hourly_df.select(["id", "DATE", "year", "month", "SLP"])
    temp2_df = temp_df.groupBy(["id", "DATE", "year", "month"])\
        .agg(sf.round(sf.avg("SLP"), 2).alias("avgSLP"),
            sf.round(sf.min("SLP"), 2).alias("minSLP"),
            sf.round(sf.max("SLP"), 2).alias("maxSLP"))
    computed_df = computed_df.join(temp2_df, ["id", "DATE", "year", "month"])
    computed_df = computed_df.repartition("year", "month")
    return computed_df

def process_monthly(daily_df):
    #----- Compute monthly metrics ------
    # temperature
    temp_df = daily_df.select(["id", "year", "month", "avgTMP", "minTMP","maxTMP"])
    temp2_df = temp_df.groupBy(["id", "year", "month"])\
        .agg(sf.round(sf.avg("avgTMP"), 2).alias("avgTMP"),
             sf.round(sf.avg("minTMP"), 2).alias("avg_minTMP"),
             sf.round(sf.min("minTMP"), 2).alias("minTMP"),
             sf.round(sf.avg("maxTMP"), 2).alias("avg_maxTMP"),
            sf.round(sf.max("maxTMP"), 2).alias("maxTMP"))
    monthly_df = temp2_df

    # Dew Point
    temp_df = daily_df.select(["id", "year", "month", "avgDEW", "minDEW","maxDEW"])
    temp2_df = temp_df.groupBy(["id", "year", "month"])\
        .agg(sf.round(sf.avg("avgDEW"), 2).alias("avgDEW"),
            sf.round(sf.avg("minDEW"), 2).alias("avg_minDEW"),
            sf.round(sf.min("minDEW"), 2).alias("minDEW"),
            sf.round(sf.avg("maxDEW"), 2).alias("avg_maxDEW"),
            sf.round(sf.max("maxDEW"), 2).alias("maxDEW"))
    monthly_df = monthly_df.join(temp2_df, ["id", "year", "month"])

    # Wind Rate
    temp_df = daily_df.select(["id", "year", "month", "avgWIND", "minWIND","maxWIND"])
    temp2_df = temp_df.groupBy(["id", "year", "month"])\
        .agg(sf.round(sf.avg("avgWIND"), 2).alias("avgWIND"),
             sf.round(sf.avg("minWIND"), 2).alias("avg_minWIND"),
            sf.round(sf.min("minWIND"), 2).alias("minWIND"),
            sf.round(sf.avg("maxWIND"), 2).alias("avg_maxWIND"),
            sf.round(sf.max("maxWIND"), 2).alias("maxWIND"))
    monthly_df = monthly_df.join(temp2_df, ["id", "year", "month"])

    # Sea Level Pressure
    temp_df = daily_df.select(["id", "year", "month", "avgSLP", "minSLP","maxSLP"])
    temp2_df = temp_df.groupBy(["id", "year", "month"])\
        .agg(sf.round(sf.avg("avgSLP"), 2).alias("avgSLP"),
            sf.round(sf.avg("minSLP"), 2).alias("avg_minSLP"),
            sf.round(sf.min("minSLP"), 2).alias("minSLP"),
            sf.round(sf.avg("maxSLP"), 2).alias("avg_maxSLP"),
            sf.round(sf.max("maxSLP"), 2).alias("maxSLP"))
    monthly_df = monthly_df.join(temp2_df, ["id", "year", "month"])
    monthly_df = monthly_df.repartition("year", "month")
    return monthly_df

def process_yearly(daily_df):
    # Compute from daily table
    # temperature
    temp_df = daily_df.select(["id", "year", "avgTMP", "minTMP","maxTMP"])
    temp2_df = temp_df.groupBy(["id", "year"])\
        .agg(sf.round(sf.avg("avgTMP"), 2).alias("avgTMP"),
             sf.round(sf.avg("minTMP"), 2).alias("avg_minTMP"),
             sf.round(sf.min("minTMP"), 2).alias("minTMP"),
             sf.round(sf.avg("maxTMP"), 2).alias("avg_maxTMP"),
            sf.round(sf.max("maxTMP"), 2).alias("maxTMP"))
    yearly_df = temp2_df

    # Dew Point
    temp_df = daily_df.select(["id", "year", "avgDEW", "minDEW","maxDEW"])
    temp2_df = temp_df.groupBy(["id", "year"])\
        .agg(sf.round(sf.avg("avgDEW"), 2).alias("avgDEW"),
             sf.round(sf.avg("minDEW"), 2).alias("avg_minDEW"),
            sf.round(sf.min("minDEW"), 2).alias("minDEW"),
            sf.round(sf.avg("maxDEW"), 2).alias("avg_maxDEW"),
            sf.round(sf.max("maxDEW"), 2).alias("maxDEW"))
    yearly_df = yearly_df.join(temp2_df, ["id", "year"])

    # Wind Rate
    temp_df = daily_df.select(["id", "year", "avgWIND", "minWIND", "maxWIND"])
    temp2_df = temp_df.groupBy(["id", "year"])\
        .agg(sf.round(sf.avg("avgWIND"), 2).alias("avgWIND"),
            sf.round(sf.avg("minWIND"), 2).alias("avg_minWIND"),
            sf.round(sf.min("minWIND"), 2).alias("minWIND"),
            sf.round(sf.avg("maxWIND"), 2).alias("avg_maxWIND"),
            sf.round(sf.max("maxWIND"), 2).alias("maxWIND"))
    yearly_df = yearly_df.join(temp2_df, ["id", "year"])

    # Sea Level Pressure
    temp_df = daily_df.select(["id", "year", "avgSLP", "minSLP", "maxSLP"])
    temp2_df = temp_df.groupBy(["id", "year"])\
        .agg(sf.round(sf.avg("avgSLP"), 2).alias("avgSLP"),
            sf.round(sf.avg("minSLP"), 2).alias("avg_minSLP"),
            sf.round(sf.min("minSLP"), 2).alias("minSLP"),
            sf.round(sf.avg("maxSLP"), 2).alias("avg_maxSLP"),
            sf.round(sf.max("maxSLP"), 2).alias("maxSLP"))
    yearly_df = yearly_df.join(temp2_df, ["id", "year"])
    yearly_df = yearly_df.repartition(1, "year")
    return yearly_df

def generate_map(df, path):
    """Generate the location map JSON using the Dataframe df that stores the location information and save to the path"""
    import plotly.express as pex
    print("Generating location map ...")
    pdf = df.toPandas()
    fig = pex.scatter_geo(pdf,lat="LATITUDE",lon="LONGITUDE",
                        hover_name="city",
                        hover_data=["id","LATITUDE","LONGITUDE"])
    fig.update_geos(showcountries = True)
    f = open(path, "w")
    fig.write_json(f)


# ====== 
# Read and Write the Dataframes 
# ======
def preprocess_data():
    source_df = spark.read.parquet(data_path)
    # ---Write Hourly Data -------
    hourly_df, location_df = process_hourly_locs(spark, source_df)
    print("writing hourly ordered table")
    hourly_df.write.parquet(hourly_path, mode="overwrite",
                            partitionBy=["year", "month"])
    print("writing location table")
    location_df.write.parquet(location_path, mode="overwrite")
    
    location_df = spark.read.parquet(location_path)
    generate_map(location_df, location_map_path) # generate the map for location id finding

    hourly_df = spark.read.parquet(hourly_path) # speed up later computation by using the written data

    #---Write Daily Data---------
    daily_df = process_daily(hourly_df)
    print("Writing daily metric table")
    daily_df.write.parquet(daily_path, mode="overwrite",
                        partitionBy=["year", "month"])

    daily_df = spark.read.parquet(daily_path) # speed up later computation by using the written data

    #--- Write Monthly Data -----
    monthly_df = process_monthly(daily_df)
    print("Writing monthly metric table...")
    monthly_df.write.parquet(monthly_path, mode="overwrite",
                        partitionBy=["year", "month"])
    
    #--- Write Yearly Data ----
    yearly_df = process_yearly(daily_df)
    yearly_df.show(5)
    print("Writing yearly table...")
    yearly_df.write.parquet(yearly_path, mode="overwrite")


if __name__ == "__main__":
    
    preprocess_data()

spark.stop()