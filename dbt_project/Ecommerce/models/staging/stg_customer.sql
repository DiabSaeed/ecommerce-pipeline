{{ 
    config(
        materialized='table',
        schema='STAGING'
    )
}}

with customers as (
    select 
        cast(CUSTOMER_ID as int) as customer_id,
        cast(FIRST_NAME as varchar) as first_name,
        cast(EMAIL as varchar) as email,
        trim(cast(CITY as varchar)) as city,
        cast(REGISTRATION_DATE as date) as reg_date,
        initcap(cast(LOYALTY_SEGMENT as varchar)) as loyalty_seg
    from {{ source('ecommerce', 'customers') }}
)

select *
from customers