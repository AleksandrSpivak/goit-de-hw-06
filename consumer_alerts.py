# Читає з AS_topic_out алерти та виводить в консоль для підтвердження запису в топік

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType, StructField, TimestampType, DoubleType

os.environ[
    'PYSPARK_SUBMIT_ARGS'
] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

spark = (
    SparkSession.builder
    .appName("KafkaConsumer")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")  
    .getOrCreate()
)

schema = StructType([
    StructField("window", StructType([
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True),
    ])),
    StructField("t_avg", DoubleType(), True),
    StructField("h_avg", DoubleType(), True),
    StructField("code", StringType(), True),
    StructField("message", StringType(), True),
    StructField("timestamp", StringType(), True)
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "77.81.230.104:9092")
    .option("subscribe", "AS_topic_out")
    .option("startingOffsets", "earliest")
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';")
    .load()
)

deserialized_df = df.selectExpr("CAST(value AS STRING) AS value").select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

query = (
    deserialized_df.writeStream
    .outputMode("append")
    .format("console")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()

