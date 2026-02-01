from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType 


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ModelCreation") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Define the schema for car models
schema = StructType([
    StructField("model_id", IntegerType(), False),
    StructField("car_brand", StringType(), False),
    StructField("car_model", StringType(), False)
])

# Sample data for car models
data = [
    (1,"Mazda",3),
    (2,"Mazda",6),
    (3,"Toyota","Corolla"),
    (4,"Hyundai","i20"),
    (5,"Kia","Sportage"),
    (6,"Kia","Rio"),
    (7,"Kia","Picanto")
]

df_cars =spark.createDataFrame(data, schema)

s3_path = 's3a://spark/data/dims/car_models/'

print("Writing car models data to S3 path:", s3_path)

df_cars.coalesce(1).write.mode('overwrite')\
    .option("header", "true") \
    .csv(s3_path)

print("Car models data written successfully.")
