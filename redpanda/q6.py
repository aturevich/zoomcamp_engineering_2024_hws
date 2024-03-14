from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

pyspark_version = '3.3.2'  
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = SparkSession \
    .builder \
    .appName("GreenTripsConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

df_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

schema = StructType([
    StructField("lpep_pickup_datetime", StringType()),
    StructField("lpep_dropoff_datetime", StringType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("passenger_count", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("tip_amount", DoubleType()),
])

df_parsed = df_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

batch_count = 0

def foreach_batch_function(df, epoch_id):
    global batch_count
    if batch_count == 0:
        df.show()  
        batch_count += 1
    else:
        query.stop()

query = df_parsed.writeStream.foreachBatch(foreach_batch_function).start()

query.awaitTermination()
