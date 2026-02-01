from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql import types as T
from datetime import datetime 
from pyspark.sql.types import StringType, StructType, IntegerType, FloatType, LongType
import kafka
from pyspark.sql.window import Window

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


#step 1 : read data from kafka topic sensors-sample

df_kafka_alert_data = spark\
    .readStream\
    .format("kafka")\
    .option("startingOffsets", "earliest")\
    .option("kafka.bootstrap.servers", "course-kafka:9092")\
    .option("subscribe", "alert-data")\
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

df_kafka_alert_data_json_to_df = df_kafka_alert_data.select(F.col("value").cast("string"),F.col("timestamp"))\
    .select(F.from_json(F.col("value"), schema).alias("value"),F.col("timestamp"))\
    .select("value.*",F.col("timestamp"))
    
#step 3 : group alert data by driver_id and count alerts
df_kafka_alert_data_grouped = df_kafka_alert_data_json_to_df \
    .groupBy(F.window("timestamp","1 minutes"))\
    .agg(
        F.count("*").alias("number_of_rows"),
        F.sum(F.when(F.col("color_name") == "Black", 1).otherwise(0)).alias("num_of_black"),
        F.sum(F.when(F.col("color_name") == "White", 1).otherwise(0)).alias("num_of_white"),
        F.sum(F.when(F.col("color_name") == "Silver", 1).otherwise(0)).alias("num_of_silver"),
        F.max(F.col("speed")).alias("maximum_speed"),
        F.max(F.col("gear")).alias("maximum_gear"),
        F.max(F.col("rpm")).alias("maximum_rpm")
    )     


query_console = df_kafka_alert_data_grouped \
.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='1 minute') \
    .options(numRows=50) \
    .start()\
    .awaitTermination(120)  # for using the dag file time limit



#{"driver_id":"253002338","brand_name":"Toyota","model_name":"Corolla"
# ,"color_name":"Blue","gear":1,"expected_gear":5,"speed":168,"rpm":6863}
