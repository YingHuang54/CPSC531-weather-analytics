# Frontend address to prevent CORS blocking it
FRONTEND_URL = "http://localhost:5173"

# Flask app port
PORT = 5000

## Spark Configuration
# spark master for the preprocessing scripts
PRE_SPARK_MASTER = "local[3]" 
# spark master for the backend app, less to leave some cpu power for the flask app if needed
SPARK_MASTER = "local[2]" 
DRIVER_MEMORY = "2g"
EXECUTOR_MEMORY = "2g"

## Data locations
# raw csv data location in Spark's configured filesystem
RAW_PATH = "/data/whole" 

# directory in Spark's configured filesystem to write the parquet data directly converted from raw csv
PARQUET_PATH = "/data/whole_parquet" 

# Directory for precomputed tables in Spark's configured filesystem, each go to different subfolder under this directory
PRECOMPUTED_PATH = "/data/whole_processed" 

# path for location map Json in local storage (not Spark's configured filesystem)
LOCATION_MAP_PATH = "./location_map.json"

# The path to replicate small file problem encountered, in Spark's configured filesystem
TINY_PATH = "/data/whole_small"
# The path to use repartition on all tables after small file problem encountered, in Spark's configured filesystem
LARGER_PATH = "/data/whole_larger"

# for small subset of original csv files to compare the preprocessing on csv or converted to parquet before preprocessing
# path in Spark's configured filesystem
SMALL_CSV = "file:///home/ying/project531/data/small"
SMALL_PARQUET = "file:///home/ying/project531/data/small_parquet"
SMALL_COMPUTED = "file:///home/ying/project531/data/small_computed"