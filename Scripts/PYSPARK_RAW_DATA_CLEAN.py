# glue_clean_and_parquet_fixed.py
"""
Reads raw CSV from S3 -> cleans -> transforms -> writes Parquet to cleaned/ paths
Glue 3.0 / Spark 3.3 compatible
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql import types as T
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

# -------------------------------------
# Args
# -------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME','BUCKET','ENV'])
bucket = args['BUCKET']
env = args['ENV']

raw_prefix   = f"s3://{bucket}/raw"
clean_prefix = f"s3://{bucket}/cleaned"
temp_dir     = f"s3://{bucket}/glue-temp/"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

logger.info(f"===== JOB START =====")
print(f"===== JOB START =====")
logger.info(f"ENV={env}, BUCKET={bucket}")
print(f"ENV={env}, BUCKET={bucket}")
logger.info(f"RAW={raw_prefix}, CLEANED={clean_prefix}")
print((f"RAW={raw_prefix}, CLEANED={clean_prefix}"))

# -------------------------------------
# Helper Config
# -------------------------------------
HIGH_COST_THRESHOLD_BILL = 20000.0
HIGH_TXN_AMOUNT = 5000.0

gender_map = {'M':'M', 'Male':'M','F':'F','Female':'F','O':'O','Other':'O'}

def standardize_gender(df, col='gender'):
    return (
        df.withColumn(col, F.when(F.col(col).isNull(), F.lit('U')).otherwise(F.col(col)))
          .withColumn(col, F.initcap(F.col(col)))
          .replace(gender_map, subset=[col])
    )

# =====================================
print("\n==================== MEMBERS ====================")
# =====================================
members_path = f"{raw_prefix}/Members/*.csv"
logger.info(f"Reading MEMBERS: {members_path}")
print(f"Reading MEMBERS: {members_path}")

members_schema = T.StructType([
    T.StructField("member_id", T.StringType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField("age", T.IntegerType(), True),
    T.StructField("gender", T.StringType(), True),
    T.StructField("city", T.StringType(), True),
    T.StructField("join_date", T.StringType(), True),
    T.StructField("member_status", T.StringType(), True),
    T.StructField("income_bracket", T.StringType(), True)
])

print("[READ] Loading members CSV with explicit schema...")
df_members = (
    spark.read.option("header", True)
    .schema(members_schema)
    .csv("s3://member-risk-data/raw/Members/*.csv")
)

# Clean + cast
df_members = (
    df_members
    .withColumn("member_id", F.col("member_id").cast("long"))
    .withColumn("name", F.trim(F.col("name")))
    .transform(standardize_gender)
    .withColumn("city", F.initcap(F.col("city")))
    .withColumn("join_date", F.to_date("join_date", "yyyy-MM-dd"))
    .withColumn("member_status", F.coalesce("member_status", F.lit("Active")))
    .withColumn("income_bracket", F.coalesce("income_bracket", F.lit("Unknown")))
    .withColumn("load_time", F.current_timestamp())
)

df_members = df_members.dropDuplicates(["member_id"])

print(f"[INFO] Members row count: {df_members.count()}")

# Add join-year partition
df_members = df_members.withColumn("join_year", F.year("join_date"))

members_out = f"{clean_prefix}/Members/"
print(f"[WRITE] Writing members parquet to: {members_out}")

df_members.write.mode("overwrite").parquet(members_out)

# =====================================
print("\n==================== VISITS ====================")
# =====================================
visits_path = f"{raw_prefix}/Visits/*.csv"
print(f"Reading VISITS: {visits_path}")

visits_schema = T.StructType([
    T.StructField("visit_id", T.StringType(), True),
    T.StructField("member_id", T.StringType(), True),
    T.StructField("date", T.StringType(), True),
    T.StructField("diagnosis", T.StringType(), True),
    T.StructField("billed_amt", T.StringType(), True),
    T.StructField("paid_amt", T.StringType(), True),
    T.StructField("provider", T.StringType(), True),
    T.StructField("is_emergency", T.StringType(), True),
    T.StructField("claim_status", T.StringType(), True)
])

print("[READ] Loading visits CSV...")
df_visits = (
    spark.read.option("header", True)
    .schema(visits_schema)
    .csv(visits_path)
)

# Clean + cast
df_visits = (
    df_visits
    .withColumn("visit_id", F.col("visit_id").cast("long"))
    .withColumn("member_id", F.col("member_id").cast("long"))
    .withColumn("visit_date", F.to_date("date", "yyyy-MM-dd"))
    .withColumn("diagnosis", F.initcap("diagnosis"))
    .withColumn("billed_amt", F.col("billed_amt").cast("double"))
    .withColumn("paid_amt", F.col("paid_amt").cast("double"))
    .withColumn("provider", F.initcap("provider"))
    .withColumn(
        "is_emergency",
        F.when(F.col("is_emergency").isin("1", "true", "True"), 1).otherwise(0)
    )
    .withColumn("claim_status", F.initcap("claim_status"))
    .withColumn("bill_gap", F.col("billed_amt") - F.col("paid_amt"))
    .withColumn(
        "is_high_cost",
        F.when(F.col("billed_amt") >= HIGH_COST_THRESHOLD_BILL, 1).otherwise(0)
    )
    .withColumn("load_time", F.current_timestamp())
)

df_visits = df_visits.filter(
    (F.col("member_id").isNotNull()) & (F.col("visit_date").isNotNull())
)

print(f"[INFO] Visits row count: {df_visits.count()}")

# Add partitions
df_visits = (
    df_visits.withColumn("year", F.year("visit_date"))
             .withColumn("month", F.month("visit_date"))
)

# JOIN with members for enrichment
print("[JOIN] Joining visits with members...")
df_visits = df_visits.join(
    df_members.select("member_id", "age", "gender", "income_bracket"),
    on="member_id",
    how="left"
)

visits_out = f"{clean_prefix}/Visits/"
print(f"[WRITE] Writing visits parquet to {visits_out} partitioned by year/month")

(
    df_visits.repartition("year", "month")
             .write.mode("overwrite")
             .partitionBy("year", "month")
             .parquet(visits_out)
)


# =====================================
print("\n==================== TRANSACTIONS ====================")
# =====================================
txns_path = f"{raw_prefix}/Transactions/*.csv"
print(f"Reading TRANSACTIONS: {txns_path}")

txns_schema = T.StructType([
    T.StructField("txn_id", T.StringType(), True),
    T.StructField("member_id", T.StringType(), True),
    T.StructField("txn_date", T.StringType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("amount", T.StringType(), True),
    T.StructField("payment_method", T.StringType(), True),
    T.StructField("merchant", T.StringType(), True),
    T.StructField("is_international", T.StringType(), True)
])

print("[READ] Loading transactions CSV...")
df_txns = (
    spark.read.option("header", True)
    .schema(txns_schema)
    .csv(txns_path)
)

# Clean + cast
df_txns = (
    df_txns
    .withColumn("txn_id", F.col("txn_id").cast("long"))
    .withColumn("member_id", F.col("member_id").cast("long"))
    .withColumn("txn_date", F.to_date("txn_date", "yyyy-MM-dd"))
    .withColumn("category", F.initcap("category"))
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("payment_method", F.initcap("payment_method"))
    .withColumn("merchant", F.initcap("merchant"))
    .withColumn(
        "is_international",
        F.when(F.col("is_international").isin("1", "true", "True"), 1).otherwise(0)
    )
    .withColumn(
        "is_large_txn",
        F.when(F.col("amount") >= HIGH_TXN_AMOUNT, 1).otherwise(0)
    )
    .withColumn("load_time", F.current_timestamp())
)

df_txns = df_txns.filter(
    (F.col("member_id").isNotNull()) & (F.col("txn_date").isNotNull())
)

print(f"[INFO] Transactions row count: {df_txns.count()}")

# Add partitions
df_txns = (
    df_txns.withColumn("year", F.year("txn_date"))
           .withColumn("month", F.month("txn_date"))
)

# Flag suspicious merchants
df_txns = df_txns.withColumn(
    "merchant_flag",
    F.when(F.col("merchant").rlike("(?i)fraud|suspect|unknown"), 1).otherwise(0)
)

txns_out = f"{clean_prefix}/Transactions/"
print(f"[WRITE] Writing transactions parquet to {txns_out} partitioned by year/month")

(
    df_txns.repartition("year", "month")
           .write.mode("overwrite")
           .partitionBy("year", "month")
           .parquet(txns_out)
)

print("===== JOB DONE SUCCESSFULLY =====")
