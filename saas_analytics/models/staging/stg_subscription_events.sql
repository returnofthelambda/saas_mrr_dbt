-- saas_analytics/models/staging/stg_subscription_events.sql
with source as (
    select * from {{ source('raw', 'subscription_events') }}
),

renamed as (
    select
        cast(event_id as varchar) as event_id,
        cast(customer_id as varchar) as customer_id,
        cast(event_type as varchar) as event_type,
        cast(event_date as date) as event_date,
        cast(mrr_change as numeric(16,2)) as mrr_change_amount,
        cast(current_mrr as numeric(16,2)) as ending_mrr_amount
    from source
)

select * from renamed
