{{
    config(
        materialized='table'
    )
}}

with fhv_table as (
    select *
    from {{ ref('stg_fhv_table') }}

),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    fhv_table.pickup_datetime, 
    fhv_table.dropoff_datetime, 
    fhv_table.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_table.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,   
    fhv_table.dispatching_base_num,
    fhv_table.affiliated_base_number,
    fhv_table.sr_flag
from fhv_table
inner join dim_zones as pickup_zone
on fhv_table.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_table.dropoff_locationid = dropoff_zone.locationid

