{{config(materialized="incremental",
        unique_key="member_id",
        incremental_strategy="merge"
        )}}

SELECT 
    member_id,
    name,
    age,
    gender,
    city,
    join_date,
    member_status,
    income_bracket,
    load_time as source_load_time,
    year(join_date) as join_year,
    current_timestamp() as modified_time
 FROM {{source("analytics_stg","members")}}

 {% if is_incremental()%}
    where load_time > (select coalesce(max(source_load_time),'1900-01-01') from {{this}})
 {% endif%}