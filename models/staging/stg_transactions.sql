select
    TXN_ID,
    MEMBER_ID,
    TXN_DATE,
    CATEGORY,
    AMOUNT,
    PAYMENT_METHOD,
    MERCHANT,
    IS_INTERNATIONAL,
    IS_LARGE_TXN,
    LOAD_TIME
from {{source("analytics_stg","transactions")}}