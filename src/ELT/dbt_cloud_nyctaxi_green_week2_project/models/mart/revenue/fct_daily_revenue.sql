{{ config(materialized="table", schema="fct") }}
SELECT
  FORMAT_DATETIME('%F', dropoff_datetime) AS dropoff_date,
  ROUND(SUM(total_amount), 2) AS revenue_daily_total_amount

FROM {{ref ('stg_green_taxi')}}
WHERE FORMAT_DATETIME('%G', dropoff_datetime) ='2021'
GROUP BY dropoff_date
ORDER BY dropoff_date
