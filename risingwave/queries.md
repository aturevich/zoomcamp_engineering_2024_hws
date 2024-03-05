## Question 1

[](https://github.com/risingwavelabs/risingwave-data-talks-workshop-2024-03-04/blob/main/homework.md#question-1)

Create a materialized view to compute the average, min and max trip time between each taxi zone.

From this MV, find the pair of taxi zones with the highest average trip time. You may need to use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) for this.

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute, but the max trip time is 10 minutes and 20 minutes respectively.

```sql
CREATE MATERIALIZED VIEW taxi_zone_trip_times AS
SELECT 
    td.PULocationID, 
    tz1.zone AS pickup_zone_name,
    td.DOLocationID, 
    tz2.zone AS dropoff_zone_name,
    COUNT(*) AS number_of_trips,
    AVG(EXTRACT(EPOCH FROM (td.tpep_dropoff_datetime - td.tpep_pickup_datetime)) / 60) AS avg_trip_time_minutes,
    MIN(EXTRACT(EPOCH FROM (td.tpep_dropoff_datetime - td.tpep_pickup_datetime)) / 60) AS min_trip_time_minutes,
    MAX(EXTRACT(EPOCH FROM (td.tpep_dropoff_datetime - td.tpep_pickup_datetime)) / 60) AS max_trip_time_minutes
FROM 
    trip_data td
JOIN 
    taxi_zone tz1 ON td.PULocationID = tz1.location_id
JOIN 
    taxi_zone tz2 ON td.DOLocationID = tz2.location_id
GROUP BY 
    td.PULocationID, tz1.zone, td.DOLocationID, tz2.zone;
```

## Question 2

[](https://github.com/risingwavelabs/risingwave-data-talks-workshop-2024-03-04/blob/main/homework.md#question-2)

Recreate the MV(s) in question 1, to also find the number of trips for the pair of taxi zones with the highest average trip time.

```sql
SELECT 
    PULocationID, 
    pickup_zone_name,
    DOLocationID, 
    dropoff_zone_name,
    number_of_trips,
    avg_trip_time_minutes
FROM 
    taxi_zone_trip_times
ORDER BY 
    avg_trip_time_minutes DESC
LIMIT 1;
```

## Question 3

[](https://github.com/risingwavelabs/risingwave-data-talks-workshop-2024-03-04/blob/main/homework.md#question-3)

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups? For example if the latest pickup time is 2020-01-01 12:00:00, then the query should return the top 3 busiest zones from 2020-01-01 11:00:00 to 2020-01-01 12:00:00.

HINT: You can use [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) to create a filter condition based on the latest pickup time.

NOTE: For this question `17 hours` was picked to ensure we have enough data to work with.

```sql
WITH latest_pickup AS (
    SELECT MAX(tpep_pickup_datetime) AS max_pickup_time
    FROM trip_data
), pickup_window AS (
    SELECT
        tpep_pickup_datetime,
        PULocationID
    FROM
        trip_data,
        latest_pickup
    WHERE
        tpep_pickup_datetime BETWEEN (latest_pickup.max_pickup_time - INTERVAL '17 HOURS') AND latest_pickup.max_pickup_time
), pickup_counts AS (
    SELECT
        pw.PULocationID,
        COUNT(*) AS num_pickups
    FROM
        pickup_window pw
    GROUP BY
        pw.PULocationID
    ORDER BY
        num_pickups DESC
    LIMIT 3
), ranked_zones AS (
    SELECT
        pc.PULocationID,
        tz.zone AS pickup_zone_name,
        pc.num_pickups
    FROM
        pickup_counts pc
    JOIN
        taxi_zone tz ON pc.PULocationID = tz.location_id
)
SELECT * FROM ranked_zones;
```