from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql import types as T
from datetime import datetime 
import kafka
import json
import time
import uuid
import random

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('DataGenerator') \
    .config("spark.driver.memory", "4g") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

load_csv_car_data = spark.read.option("header", "true").csv('s3a://spark/data/dims/cars/')


kafka_producer = kafka.KafkaProducer(bootstrap_servers='course-kafka:9092',
                                     value_serializer=lambda x: json.dumps(x).encode('utf-8'),acks=1)

start_time = time.time()
execution_limit_seconds = 50


while True: 
        
        #  using the dag file time limit
        if time.time() - start_time > execution_limit_seconds:
           print("Total time limit reached. Stopping.")
           break

        load_csv_car_data_df  = load_csv_car_data \
            .withColumn("event_id",F.expr("uuid()")) \
            .withColumn("event_time", F.current_timestamp().cast("string")) \
            .withColumn("speed", (F.floor(F.rand() * 201)).cast("int")) \
            .withColumn("rpm", (F.floor(F.rand() * 8000)).cast("int")) \
            .withColumn("gear", (F.floor(F.rand() * 7)+1).cast("int"))
        
        #load_csv_car_data_df.show(50)

                
        for row in load_csv_car_data_df.collect():  
                #  using the dag file time limit
            if time.time() - start_time > execution_limit_seconds:  
                     break
            record = row.asDict()
            kafka_producer.send(topic='sensors-sample', value=record)
            print(f"Sent record {row} to Kafka")        
        time.sleep(1)  # Simulate delay between messages



