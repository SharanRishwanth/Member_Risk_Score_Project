import sys
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T


# --------- Fetch parameters from SSM ----------
def get_param(name):
    return boto3.client("ssm").get_parameter(
        Name=name, WithDecryption=True
    )["Parameter"]["Value"]


# --------- Glue Job Arguments ----------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job_name = args["JOB_NAME"]

# --------- Spark + Glue Initialization ----------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
spark.conf.set("spark.local.dir", "/tmp/glue-temp")
spark.conf.set("spark.hadoop.tmp.dir", "/tmp/glue-temp")
spark.conf.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")

print(f"===== STARTING GLUE JOB: {job_name} =====")


# --------- Load Snowflake Secrets ----------
print("Fetching Snowflake credentials")

sfOptions = {
    "sfURL": f"{get_param('/snowflake/account')}.snowflakecomputing.com",
    "sfUser": get_param("/snowflake/user"),
    "sfPassword": get_param("/snowflake/password"),
    "sfWarehouse": get_param("/snowflake/warehouse"),
    "sfDatabase": get_param("/snowflake/database"),
    "sfSchema": get_param("/snowflake/schema"),
}


# --------- Parquet Datasets to Load ----------
datasets = [
    {"path": "s3://member-risk-data/cleaned/Members/",      "table": "MEMBERS"},
    {"path": "s3://member-risk-data/cleaned/Transactions/", "table": "TRANSACTIONS"},
    {"path": "s3://member-risk-data/cleaned/Visits/",       "table": "VISITS"},
]


# --------- Load Each Dataset ----------
for d in datasets:
    path = d["path"]
    table = d["table"]

    print(f"Reading parquet from: {path}")

    try:
        df = spark.read.parquet(path)
        print("Columns:", df.columns)

        # Clean column names (uppercase + replace dots)
        for c in df.columns:
            df = df.withColumnRenamed(c, c.replace(".", "_").upper())

        # Fix NullType columns
        for field in df.schema:
            if isinstance(field.dataType, T.NullType):
                df = df.withColumn(field.name, F.lit(None).cast(T.StringType()))

        print(f"Writing to Snowflake → {table}")

        (
            df.write
            .format("snowflake")
            .options(**sfOptions)
            .option("dbtable", table)
            .mode("overwrite")  # Change to append if needed
            .save()
        )

        print(f"SUCCESS → {table}")

    except Exception as e:
        print(f"FAILED → {table}: {e}")
        logger.error(f"FAILED → {table}: {e}")


print("===== JOB COMPLETE =====")

