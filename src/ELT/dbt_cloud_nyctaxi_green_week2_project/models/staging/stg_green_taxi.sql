{{ config(materialized="table", schema="stg") }}


select
    VendorID as vendor_id,
    lpep_pickup_datetime as pickup_datetime,
    lpep_dropoff_datetime as dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID as rate_code_id,
    store_and_fwd_flag,
    PULocationID as pickup_location_id,
    DOLocationID as dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    trip_type
from {{ source("src_green_taxi" ,"merged_tripdata_2021")}}

{% if target.name == 'dev' %}
limit 100
{% endif %}
