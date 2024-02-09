create external table `zoomcamp-de-413712.nytaxi.green_taxi_22`
options (
  format = 'parquet',
  uris = ['gs://nytaxi-de/green_taxi_2022/green_tripdata_2022-*.parquet']
); 


SELECT COUNT(*) AS total_records
FROM `zoomcamp-de-413712.nytaxi.green_taxi_22`;


CREATE OR REPLACE TABLE zoomcamp-de-413712.nytaxi.green_tripdata_non_partitioned AS
SELECT * FROM zoomcamp-de-413712.nytaxi.green_taxi_22;

CREATE OR REPLACE TABLE zoomcamp-de-413712.nytaxi.green_tripdata_partitioned
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM zoomcamp-de-413712.nytaxi.green_taxi_22;

SELECT COUNT(DISTINCT(PULocationID))  FROM `zoomcamp-de-413712.nytaxi.green_tripdata_non_partitioned`;

SELECT COUNT(DISTINCT(PULocationID)) FROM `zoomcamp-de-413712.nytaxi.green_taxi_22`;

SELECT COUNT(*) AS total_records
FROM `zoomcamp-de-413712.nytaxi.green_taxi_22`
WHERE fare_amount = 0;