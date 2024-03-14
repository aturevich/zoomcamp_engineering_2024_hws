from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import window, current_timestamp

pyspark_version = '3.3.2'  
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = SparkSession \
    .builder \
    .appName("GreenTripsConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()


df_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .load()


schema = StructType() \
    .add("lpep_pickup_datetime", StringType()) \
    .add("lpep_dropoff_datetime", StringType()) \
    .add("PULocationID", IntegerType()) \
    .add("DOLocationID", IntegerType()) \
    .add("passenger_count", DoubleType()) \
    .add("trip_distance", DoubleType()) \
    .add("tip_amount", DoubleType())

df_parsed = df_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

df_with_timestamp = df_parsed.withColumn("timestamp", current_timestamp())

popular_destinations = df_with_timestamp \
    .groupBy(window(col("timestamp"), "5 minutes"), col("DOLocationID")) \
    .count() \
    .orderBy("count", ascending=False)

query = popular_destinations \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()