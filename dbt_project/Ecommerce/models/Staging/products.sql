{{
config(
    materialized='table',
    schema='STAGING'
)
}}

with products as (
select *
from {{source('ecommerce','products')}}
)
select *, CURRENT_TIMESTAMP as ingestion_DateTime
FROM products