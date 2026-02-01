from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType 


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ColorCreation") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Define the schema for car colors
schema = StructType([
    StructField("color_id", IntegerType(), False),
    StructField("color_name", StringType(), False)
])

# Sample data for car colors
data = [
    (1,"Black"),
    (2,"Red"),
    (3,"Gray"),
    (4,"White"),
    (5,"Green"),
    (6,"Blue"),
    (7,"Pink")
]

df_cars =spark.createDataFrame(data, schema)

s3_path = 's3a://spark/data/dims/car_colors/'

print("Writing car colors data to S3 path:", s3_path)

df_cars.coalesce(1).write.mode('overwrite')\
    .option("header", "true") \
    .csv(s3_path)

print("Car colors data written successfully.")