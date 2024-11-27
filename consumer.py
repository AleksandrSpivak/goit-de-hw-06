# читає дані з AS_topic_in обробляє та пише алерти в AS_topic_out

import datetime
import uuid
import os

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import SparkSession
from configs import kafka_config

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

alerts_df = spark.read.csv("alerts_conditions.csv", header=True)


window_duration = "1 minute"
sliding_interval = "30 seconds"
watermark_duration = "10 seconds"

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", "AS_topic_in") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "300") \
    .option("failOnDataLoss", "false") \
    .load()

json_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True)
])

avg_stats = df.selectExpr("CAST(key AS STRING) AS key_deserialized", "CAST(value AS STRING) AS value_deserialized", "*") \
    .drop('key', 'value') \
    .withColumnRenamed("key_deserialized", "key") \
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema)) \
    .withColumn("timestamp", from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp")) \
    .withWatermark("timestamp", watermark_duration) \
    .groupBy(window(col("timestamp"), window_duration, sliding_interval)) \
    .agg(
    avg("value_json.temperature").alias("t_avg"),
    avg("value_json.humidity").alias("h_avg")
    ) \
    .drop("topic")
    
all_alerts = avg_stats.crossJoin(alerts_df)

# %%

valid_alerts = all_alerts \
    .where("t_avg > temperature_min AND t_avg < temperature_max") \
    .unionAll(
    all_alerts
    .where("h_avg > humidity_min AND h_avg < humidity_max")
) \
    .withColumn("timestamp", lit(str(datetime.datetime.now()))) \
    .drop("id", "humidity_min", "humidity_max", "temperature_min", "temperature_max")

# Для проміжного скріну
# displaying_df = valid_alerts.writeStream \
#     .trigger(processingTime='10 seconds') \
#     .outputMode("update") \
#     .format("console") \
#     .start() \
#     .awaitTermination()

uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

prepare_to_kafka_df = valid_alerts \
    .withColumn("key", uuid_udf()) \
    .select(
    col("key"),
    to_json(struct(col("window"),
                   col("t_avg"),
                   col("h_avg"),
                   col("code"),
                   col("message"),
                   col("timestamp"))).alias("value")
)

query = prepare_to_kafka_df.repartition(1) \
    .writeStream \
    .trigger(processingTime='15 seconds') \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", "AS_topic_out") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';") \
    .option("checkpointLocation", "/tmp/checkpoints-1") \
    .start() \
    .awaitTermination()
