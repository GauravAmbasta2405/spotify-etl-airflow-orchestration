from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


# AWS Configuration
AWS_CONN_ID = "aws_default"
AWS_REGION = "us-east-1"

# S3 Configuration
S3_BUCKET = "spotify-etl-project-gaurav-ambasta"
RAW_DATA_PREFIX = "raw_data/to_processed/"

# AWS Glue Configuration
GLUE_JOB_NAME = "spotify_transformation_job"
GLUE_SCRIPT_LOCATION = (
    "s3://aws-glue-assets-206986907456-us-east-1/"
    "scripts/spotify_transformation_job.py"
)
GLUE_IAM_ROLE = "spotify_glue_iam_role"
GLUE_ASSETS_BUCKET = "aws-glue-assets-206986907456-us-east-1"


with DAG(
    dag_id="spotify_airflow_pipeline",
    description=(
        "Orchestrates Spotify data extraction, "
        "S3 file readiness, and Glue transformation."
    ),
    start_date=datetime(2026, 11, 2),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Gaurav Ambasta",
        "depends_on_past": False,
    },
    tags=["spotify", "etl", "aws", "airflow", "glue"],
) as dag:

    # Task 1: Trigger AWS Lambda for Spotify data extraction
    trigger_extract_lambda = LambdaInvokeFunctionOperator(
        task_id="trigger_extract_lambda",
        function_name="spotify_api_data_extract",
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
    )

    # Task 2: Wait until raw JSON files are available in S3
    wait_for_raw_files = S3KeySensor(
        task_id="wait_for_raw_files",
        bucket_name=S3_BUCKET,
        bucket_key=f"{RAW_DATA_PREFIX}*",
        wildcard_match=True,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )

    # Task 3: Trigger the existing AWS Glue transformation job
    run_glue_transformation = GlueJobOperator(
        task_id="run_glue_transformation",
        job_name=GLUE_JOB_NAME,
        script_location=GLUE_SCRIPT_LOCATION,
        iam_role_name=GLUE_IAM_ROLE,
        s3_bucket=GLUE_ASSETS_BUCKET,
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        update_config=False,
        wait_for_completion=True,
    )

    # Define task dependencies
    trigger_extract_lambda >> wait_for_raw_files >> run_glue_transformation
