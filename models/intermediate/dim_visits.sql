{{config(materizlized="incremental",unique_key="visit_id",incremental_strategy="merge")}}

SELECT 
    visit_id,
    member_id,
    visit_date,
    diagnosis,
    billed_amt,
    paid_amt,
    provider,
    is_emergency,
    claim_status,
    bill_gap,
    is_high_cost,
    load_time as source_load_time,
    year(visit_date) as year,
    month(visit_date) as month,
    current_timestamp() as modified_time
FROM {{source("analytics_stg","visits")}}

{% if is_incremental() %}
    where load_time > (select coalesce(max(source_load_time),'1900-01-01') from {{this}})
{% endif %}