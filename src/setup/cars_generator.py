from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType 
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("CarGenerator") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

num_of_cars_to_generate = 20 

df_cars = spark.range(num_of_cars_to_generate+5).select(
    # car id 7 digits
    (F.floor(F.rand() * 9000000) + 1000000).cast("int").alias("car_id"),

    #deliver_id 9 digits
    (F.floor(F.rand() * 900000000) + 100000000).cast("long").alias("driver_id"),

    #model_id between 1 and 7
    (F.floor(F.rand() * 7) + 1).cast("int").alias("model_id"),

    #color_id between 1 and 7
    (F.floor(F.rand() * 7) + 1).cast("int").alias("color_id")
)

df_final_cars = df_cars.dropDuplicates(['car_id']).limit(num_of_cars_to_generate)

print(f'Generated {df_final_cars.count()} unique car records.')

print(df_final_cars.show(50))

save_path = 's3a://spark/data/dims/cars/'


df_final_cars = df_cars.dropDuplicates(['car_id']).limit(num_of_cars_to_generate)

df_final_cars.coalesce(1).write.mode('overwrite')\
    .option("header", "true") \
    .csv(save_path) 