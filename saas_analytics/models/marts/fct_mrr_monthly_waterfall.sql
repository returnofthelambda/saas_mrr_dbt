-- saas_analytics/models/marts/fct_mrr_monthly_waterfall.sql
with events as (
    select * from {{ ref('stg_subscription_events') }}
),

monthly_aggregation as (
    select
        date_trunc('month', event_date) as mrr_month,
        sum(case when event_type = 'NEW' then mrr_change_amount else 0 end) as new_mrr,
        sum(case when event_type = 'EXPANSION' then mrr_change_amount else 0 end) as expansion_mrr,
        sum(case when event_type = 'CONTRACTION' then mrr_change_amount else 0 end) as contraction_mrr,
        sum(case when event_type = 'CHURN' then mrr_change_amount else 0 end) as churn_mrr,
        sum(mrr_change_amount) as net_mrr_change
    from events
    group by 1
)

select
    mrr_month,
    new_mrr,
    expansion_mrr,
    contraction_mrr,
    churn_mrr,
    net_mrr_change,
    sum(net_mrr_change) over (order by mrr_month rows between unbounded preceding and current row) as ending_mrr
from monthly_aggregation
order by mrr_month
