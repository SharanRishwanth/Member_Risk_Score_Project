{{ config(
    materialized="incremental",
    unique_key="member_id",
    incremental_strategy="merge"
) }}

with dim_members as (
    select * 
    from {{ ref('dim_members') }}
),

dim_visits as (
    select
        member_id,
        count(*) as total_visits,
        sum(billed_amt) as total_billed,
        sum(paid_amt) as total_paid,
        sum(is_emergency) as emergency_visits,
        sum(is_high_cost) as high_cost_visits
    from {{ ref('dim_visits') }}

    -- {% if is_incremental() %}
    --     where load_time > (select max(load_time) from {{ this }})
    -- {% endif %}

    group by member_id
),

dim_transactions as (
    select
        member_id,
        count(*) as total_transactions,
        sum(amount) as total_amount,
        sum(is_large_txn) as large_txn_count,
        sum(is_international) as international_txn_count,
        sum(merchant_flag) as suspicious_merchant_txn_count
    from {{ ref('dim_transactions') }}

    -- {% if is_incremental() %}
    --     where load_time > (select max(load_time) from {{ this }})
    -- {% endif %}

    group by member_id
)

select
    m.member_id,
    m.name,
    m.age,
    m.gender,
    m.city,
    m.income_bracket,
    m.join_year,

    -- visit metrics
    coalesce(v.total_visits, 0) as total_visits,
    coalesce(v.total_billed, 0) as total_billed,
    coalesce(v.total_paid, 0) as total_paid,
    coalesce(v.emergency_visits, 0) as emergency_visits,
    coalesce(v.high_cost_visits, 0) as high_cost_visits,

    -- transaction metrics
    coalesce(t.total_transactions, 0) as total_transactions,
    coalesce(t.total_amount, 0) as total_amount,
    coalesce(t.large_txn_count, 0) as large_txn_count,
    coalesce(t.international_txn_count, 0) as international_txn_count,
    coalesce(t.suspicious_merchant_txn_count, 0) as suspicious_merchant_txn_count,

    -- derived risk score
    (
        coalesce(v.high_cost_visits, 0) * 5 +
        coalesce(v.emergency_visits, 0) * 4 +
        coalesce(t.large_txn_count, 0) * 3 +
        coalesce(t.suspicious_merchant_txn_count, 0) * 3 +
        coalesce(t.international_txn_count, 0) * 2
    ) as risk_score,

    current_timestamp() as load_time

from dim_members m
left join dim_visits v using (member_id)
left join dim_transactions t using (member_id)

-- {% if is_incremental() %}
-- where m.load_time > (select max(load_time) from {{ this }})
-- {% endif %}
