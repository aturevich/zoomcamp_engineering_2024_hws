{{
    config(
        materialized='view'
    )
}}

with fhv_tripdata as 
(
  select *
  from {{ source('staging', 'fhv_table') }}
  where dispatching_base_num is not null and
    extract(year from pickup_datetime) = 2019
)
select  
   -- identifiers
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(affiliated_base_number as string) as affiliated_base_number,
    cast(sr_flag as string) as sr_flag,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime
from fhv_tripdata 

-- -- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}