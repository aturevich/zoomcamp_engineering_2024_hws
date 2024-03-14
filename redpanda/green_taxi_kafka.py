from pyspark.sql import SparkSession
import json
from kafka import KafkaProducer

spark = SparkSession.builder.appName("GreenTaxiData").getOrCreate()

csv_file_path = "streaming/green_tripdata_2019-10.csv.gz"

df_green = spark.read.format("csv").option("header", "true").load(csv_file_path)
df_selected = df_green.select('lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount')

df_selected.show()


def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)

def send_to_kafka(row):
    message = row.asDict()
    producer.send('green-trips', value=message)

df_selected.foreach(send_to_kafka)
producer.flush()
