import sys
import os
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

# --------------------
# DEFAULT ARGUMENTS
# --------------------
default_args = {
    "owner": "member_risk_score",
    "retries": 0,
	"depends_on_past": False,
}

# --------------------
# DAG DEFINITION
# --------------------
with DAG(
    dag_id="member_risk_score_end_to_end_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 12, 19),
    schedule_interval="0 6 * * *",
    catchup=False,
	max_active_runs=1,
    tags=["python", "glue", "dbt"],
) as dag:

    # --------------------
    # START
    # --------------------
    start = EmptyOperator(task_id="start",trigger_rule="all_success")

    # --------------------
    # GENERATE INPUT DATA
    # --------------------
    def generate_csv_data():
        os.system(
        "python /mnt/c/Users/A/Documents/Member_Risk_Project/Scripts/FILE_CREATION_SCRIPT.py"
        )

    generate_intput_data = PythonOperator(
        task_id="generate_intput_data",
        python_callable=generate_csv_data,
		trigger_rule="all_success",
    )

    # --------------------
    # UPLOAD TO S3
    # --------------------
    def upload_data():
        os.system(
        "python /mnt/c/Users/A/Documents/Member_Risk_Project/Scripts/aws_s3_upload.py"
        )

    aws_s3_upload = PythonOperator(
        task_id="aws_s3_upload",
        python_callable=upload_data,
		trigger_rule="all_success",
    )

    # --------------------
    # GLUE JOB: CSV → PARQUET
    # --------------------
    glue_csv_to_parquet = GlueJobOperator(
        task_id="glue_csv_to_parquet",
        job_name="glue-raw-data-clean",
        region_name="eu-north-1",
        wait_for_completion=True,
		retries=0,                   
        trigger_rule="all_success",
    )

    # --------------------
    # GLUE JOB: PARQUET → SNOWFLAKE
    # --------------------
    glue_parquet_to_snowflake = GlueJobOperator(
        task_id="glue_parquet_to_snowflake",
        job_name="glue_sf_load",
        region_name="eu-north-1",
        wait_for_completion=True,
		retries=0,                   
        trigger_rule="all_success",
    )

    # --------------------
    # DBT BUILD
    # --------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd ~/dbt/DBT_Project &&
        dbt build
        """,
		retries=0,                  
        trigger_rule="all_success",
    )

    # --------------------
    # END
    # --------------------
    end = EmptyOperator(task_id="end",trigger_rule="all_success")

    # --------------------
    # TASK DEPENDENCIES
    # --------------------
    (
        start
        >> generate_intput_data
        >> aws_s3_upload
        >> glue_csv_to_parquet
        >> glue_parquet_to_snowflake
        >> dbt_run
        >> end
    )
	