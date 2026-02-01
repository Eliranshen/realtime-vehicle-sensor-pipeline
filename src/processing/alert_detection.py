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
    .appName('AlertDetection') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0')\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

#step 1 : read data from kafka topic samples-enriched

df_kafka_sensors_sample = spark\
    .readStream\
    .format("kafka")\
    .option("startingOffsets", "earliest")\
    .option("kafka.bootstrap.servers", "course-kafka:9092")\
    .option("subscribe", "samples-enriched")\
    .load()


#step 2 : define schema for kafka data and read to json fornat
schema = StructType() \
    .add("driver_id",StringType())\
    .add("brand_name", StringType())\
    .add("model_name",StringType())\
    .add("color_name",StringType())\
    .add("gear",IntegerType())\
    .add("expected_gear", IntegerType()) \
    .add("speed", IntegerType()) \
    .add("rpm", IntegerType()) 

#step 3 : conert kafka data to json format

df_kafka_AlertDetection_raw_json = df_kafka_sensors_sample.select(F.col("value").cast("string"))\
    .select(F.from_json(F.col("value"), schema).alias("value"))\
    .select("value.*")

#step 4 : filter alert conditions

df_kafka_AlertDetection_alert = df_kafka_AlertDetection_raw_json \
.filter(F.col("gear") != F.col("expected_gear")) \
.filter(F.col("speed") > 120)\
.filter(F.col("rpm") > 6000)

# query_console = df_kafka_AlertDetection_alert \
# .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()\
#     .awaitTermination()

#step 5 : convert alert data to json format for producer

df_kafka_AlertDetection_alert_to_json = df_kafka_AlertDetection_alert.select(
    F.to_json(F.struct("*")).alias("value"))

#step 6 : write alert data to kafka topic alert-data

    
query = df_kafka_AlertDetection_alert_to_json.writeStream\
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "alert-data") \
    .option("checkpointLocation", "s3a://spark/data/dims/checkpoint_alert_data") \
    .start()

query.awaitTermination(50) #for using the dag file time limit
# query.awaitTermination()

spark.stop()