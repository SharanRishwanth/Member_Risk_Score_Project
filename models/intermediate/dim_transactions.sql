{{config(materialized="incremental",
        unique_key="txn_id",
        incremental_strategy="merge")}}

SELECT
    txn_id,
    member_id,
    txn_date,
    category,
    amount,
    payment_method,
    merchant,
    is_international,
    is_large_txn,
    load_time as source_load_time,
    year(txn_date) as year,
    month(txn_date) as month,
    case when LOWER(merchant) IN ('fraud', 'suspect', 'unknown') then 1 else 0 end as merchant_flag,
    current_timestamp() as modified_time
FROM {{source("analytics_stg","transactions")}}

{% if is_incremental() %}
    where load_time > (select coalesce(max(source_load_time),'1900-01-01') from {{this}})
{% endif %}