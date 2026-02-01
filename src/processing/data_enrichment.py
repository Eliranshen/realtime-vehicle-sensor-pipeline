from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql import types as T
from datetime import datetime 
from pyspark.sql.types import StringType, StructType, IntegerType, FloatType, LongType
import kafka
import json
import time

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('DataEnrichment') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0')\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

#step 1 : load dimension data - join car colors, car models, cars

load_car_colors = spark.read.option("header", "true").csv('s3a://spark/data/dims/car_colors/')
load_car_models = spark.read.option("header", "true").csv('s3a://spark/data/dims/car_models/')
load_cars = spark.read.option("header", "true").csv('s3a://spark/data/dims/cars/')

cars_semi_final_df = load_cars \
    .join(load_car_colors, on='color_id') \
    .join(load_car_models, on='model_id') 
    

#step 2 : read data from kafka topic sensors-sample

df_kafka_sensors_sample = spark\
    .readStream\
    .format("kafka")\
    .option("startingOffsets", "earliest")\
    .option("kafka.bootstrap.servers", "course-kafka:9092")\
    .option("subscribe", "sensors-sample")\
    .load()


#step 3 : define schema for kafka data and read to json fornat
schema = StructType() \
    .add("event_id", StringType())\
    .add("event_time", StringType()) \
    .add("car_id", StringType()) \
    .add("speed", IntegerType()) \
    .add("rpm", IntegerType()) \
    .add("gear", IntegerType())


df_kafka_sensors_sample_json = df_kafka_sensors_sample.select(F.col("value").cast("string"))\
    .select(F.from_json(F.col("value"), schema).alias("value"))\
    .select("value.*")


#step 4 : join kafka data with car dimension data
final_df_enriched = cars_semi_final_df \
 .join(df_kafka_sensors_sample_json, on='car_id')


#step 5 : select required columns for producer
df_data_for_producer = final_df_enriched.select(
    "driver_id",
    F.col("car_brand").alias("brand_name"), 
    F.col("car_model").alias("model_name"),
    "color_name",
    "gear",
    (F.round("speed")/30).alias("expected_gear").cast("int"),
    "speed",
    "rpm"
)

# df_data_for_producer.writeStream\
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate","false") \
#     .option("numRows", "80") \
#     .start()\
#     .awaitTermination()


 #step  6: cast value to string
#df_data_for_producer_sensors_sample = df_data_for_producer.select(F.col("value").cast("string"))


df_data_for_producer_sensors_sample = df_data_for_producer.select(
    F.to_json(F.struct("*")).alias("value"))
    

#step 8 : send data to kafka topic drivers-expected-gear


query = df_data_for_producer_sensors_sample.writeStream\
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "samples-enriched") \
    .option("checkpointLocation", "s3a://spark/data/dims/checkpoint_samples_enriched") \
    .start()

query.awaitTermination(50) #for using the dag file time limit
# query.awaitTermination()


spark.stop()