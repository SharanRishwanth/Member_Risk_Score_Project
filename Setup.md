**Setup & Installation Guide**

**Project: Member Risk Score Profiling Pipeline**

**1\. System Prerequisites**

Ensure the following are available on your machine:

|   **Requirement**     |      **Version / Notes**     |
| ---                   | ---                          |
| OS                    | Windows 10/11                |
| WSL                   | WSL2 enabled                 |
| Linux Distro          | Ubuntu 20.04+                |
| Python                | 3.8 - 3.11                   |
| Git                   | Latest                       |
| AWS Account           | With Glue, S3 access         |
| Snowflake Account     | Trial or Paid                |
| Internet              | Required for AWS & Snowflake |

💡 This project is designed to run **locally on Windows via WSL2**, while interacting with **cloud-native AWS & Snowflake services**.

**2\. Ubuntu (WSL2) Setup**

**Enable WSL2**

wsl --install

Restart your system.

**Install Ubuntu**

wsl --install -d Ubuntu

Verify:

lsb_release -a

**3\. Python Environment Setup**

**Install Python & Dependencies**

sudo apt update sudo apt install python3 python3-pip python3-venv -y

**Create Virtual Environment**

python3 -m venv airflow_venv source airflow_venv/bin/activate

**Install Core Python Packages**

pip install --upgrade pip pip install boto3 pandas pyarrow snowflake-connector-python

**4\. Apache Airflow Setup (Local)**

**Install Airflow**

pip install apache-airflow==2.8.0 \\ --constraint "<https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.10.txt>"

**Initialize Airflow**

export AIRFLOW_HOME=~/airflow airflow db init

**Create Admin User**

airflow users create \\ --username admin \\ --password admin \\ --firstname Admin \\ --lastname User \\ --role Admin \\ --email <admin@example.com>

**Start Airflow**

airflow webserver -p 8080 airflow scheduler

➡ Access UI: <http://localhost:8080>

**5\. AWS Setup**

**Create IAM Role for Glue**

Required permissions:

- AmazonS3FullAccess (restricted to project bucket)
- AWSGlueServiceRole
- CloudWatch Logs
- SSM Parameter Store (Read)

**Configure AWS CLI**

aws configure

Provide:

- Access Key
- Secret Key
- Region (eu-north-1)
- Output format: json

**6\. Amazon S3 Setup**

**Create Bucket**

aws s3 mb s3://member-risk-data

**Folder Structure**

s3:_//member-risk-data/_

├── raw/

│ ├── members/

│ ├── transactions/

│ └── visits/

├── cleaned/

│ ├── members/

│ ├── transactions/

│ └── visits/

├── glue-temp/

└── scripts/

**7\. AWS Glue Setup**

**Glue Jobs Created**

|   **Job Name**        |      **Purpose**            |
|      ---              |        ---                  | 
| glue-raw-data-clean   | CSV → Parquet with PySpark  |
| glue_sf_load          | Parquet → Snowflake         |

**Glue Job Configuration**

- Glue Version: **4.0**
- Language: Python
- Workers: G.1X / G.2X
- Job Bookmark: Disabled
- Temp Directory: s3://member-risk-data/glue-temp/

**8\. Snowflake Setup**

**Create Database & Schema**

CREATE DATABASE MEMBER_RISK_DB; CREATE SCHEMA MEMBER_RISK_DB.STAGING; CREATE SCHEMA MEMBER_RISK_DB.CORE; CREATE SCHEMA MEMBER_RISK_DB.MART;

**Store Credentials in AWS SSM**

aws ssm put-parameter --name "/snowflake/user" --value "XXXX" --type SecureString aws ssm put-parameter --name "/snowflake/password" --value "XXXX" --type SecureString

**9\. dbt Core Setup**

**Install dbt**

pip install dbt-core dbt-snowflake

**Initialize Project**

dbt init DBT_Project

**dbt Profile (**profiles.yml**)**

DBT_Project: target: dev outputs: dev: type: snowflake account: &lt;account&gt; user: &lt;user&gt; password: &lt;password&gt; database: MEMBER_RISK_DB warehouse: COMPUTE_WH schema: CORE

**Run dbt**

dbt debug dbt build

**10\. Project Structure**

Member_Risk_Project/

├── Scripts/

│ ├── FILE_CREATION_SCRIPT.py

│ ├── S3_INPUT_FILE_UPLOAD.py

├── Glue/

│ ├── PYSPARK_RAW_DATA_CLEAN.py

│ └── PARQUET_TO_SNOWFLAKE_LOAD.py

├── dbt/

│ ├── models/

│ │ ├── staging/

│ │ ├── core/

│ │ └── marts/

├── airflow/

│ └── dags/

│ └── member_risk_score_end_to_end_pipeline.py

**11\. Airflow DAG Orchestration**

**DAG Flow**

Generate CSV Data

→ Upload to S3

→ Glue: CSV → Parquet

→ Glue: Parquet → Snowflake

→ dbt build

**Failure Handling**

- retries = 0
- DAG stops immediately on any failure
- Prevents unnecessary AWS Glue cost

**12\. Verification Checklist**

- CSV files generated locally
- Files uploaded to S3 raw zone
- Parquet files created in cleaned zone
- Snowflake staging tables populated
- dbt core & mart models built
- dbt tests passing
- Airflow DAG completes successfully

**13\. Design Rationale**

- Airflow chosen for orchestration flexibility and DAG-based dependency management
- AWS Glue used for scalable, serverless Spark processing
- dbt enables analytics engineering best practices (tests, lineage, versioning)
- Snowflake provides performant cloud analytics storage
- WSL2 enables Linux-native tooling on Windows without Docker dependency