Select 
    MEMBER_ID,
    NAME,
    AGE,
    GENDER,
    CITY,
    JOIN_DATE,
    MEMBER_STATUS,
    INCOME_BRACKET,
    LOAD_TIME
     from {{source("analytics_stg","members")}}