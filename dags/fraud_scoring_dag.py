# fraud_scoring_dag.py
# ---------------------------
# 🎯 Airflow DAG to trigger batch fraud scoring pipeline

import os
import sys
import boto3
import pandas as pd
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.email import EmailOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.utils.trigger_rule import TriggerRule

# Add the /workspaces/aws-fraud-detection/scripts folder to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from batch_scoring_pipeline import run_batch_scoring


# ---------------------------
# Default Configuration
# ---------------------------
session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION")
)

default_args = {
    'owner': 'Fraud Risk Team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': 'stevekwon0407@gmail.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

# ---------------------------
# Check S3 for the input file
# ---------------------------
def check_s3_input_file():
    s3 = boto3.client('s3')
    bucket_name = 'fraud-batch-pipeline-stevo'
    key = 'incoming/transactions.csv'

    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False

# ------------------------------------
# Conditional alert on high fraud rate
# ------------------------------------
def is_fraud_rate_high():
    metadata_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        'metadata',
        'scoring_metadata.csv'
    )
    if os.path.exists(metadata_path):
        df = pd.read_csv(metadata_path)
        return 'send_high_fraud_alert' if df['fraud_rate'].iloc[0] > 0.05 else 'no_alert_needed'
    return 'no_alert_needed'

# ---------------------------------
# Upload top-k flagged transactions
# ---------------------------------
def upload_top_k():
    # Reading in local metadata file
    dag_root = os.path.dirname(os.path.dirname(__file__))
    local_path = os.path.join(dag_root, 'metadata', 'scoring_results.csv')
    df = pd.read_csv(local_path)

    # Writing top K file
    df_top_k = df.sort_values('fraud_probability', ascending=False).head(100)
    local_result_path = os.path.join(dag_root, 'metadata', 'top_k_flagged.parquet')
    df_top_k.to_parquet(local_result_path, index=False)

    # Uploading to AWS S3
    s3 = boto3.client('s3')
    with open(local_result_path, 'rb') as f:
        s3.upload_fileobj(f, 'fraud-batch-pipeline-stevo', 'review/top_k_flagged.parquet')

# ---------------------------------
# Cleanup the local metadata
# ---------------------------------
def cleanup_metadata():
    dag_root = os.path.dirname(os.path.dirname(__file__))
    files = [
        'metadata/scoring_metadata.csv',
        'metadata/scoring_results.csv',
        'metadata/top_k_flagged.csv',
        'metadata/top_k_flagged.parquet'
    ]
    for file in files:
        file_path = os.path.join(dag_root, file)
        if os.path.exists(file_path):
            os.remove(file_path)

# ---------------------------------
# Athena create table
# ---------------------------------
create_table_sql = """
CREATE EXTERNAL TABLE IF NOT EXISTS fraud_top_k (
    transaction_id string,
    fraud_probability double,
    amount double,
    merchant_id string,
    customer_id string,
    timestamp timestamp
)
STORED AS PARQUET
LOCATION 's3://fraud-batch-pipeline-stevo/review/'
TBLPROPERTIES ('has_encrypted_data'='false');
"""

# ---------------------------
# Define DAG
# ---------------------------
dag = DAG(
    dag_id='fraud_scoring_batch_pipeline',
    default_args=default_args,
    description='Run batch scoring on transactions then push results to S3',
    schedule_interval='@daily',
    start_date=pendulum.now("UTC").subtract(days=1),
    catchup=False,
    tags=['fraud', 'batch', 'scoring'],
    params={}
)

check_input = ShortCircuitOperator(
    task_id='check_if_input_exists',
    python_callable=check_s3_input_file,
    dag=dag
)

run_batch = PythonOperator(
    task_id='run_batch_scoring',
    python_callable=run_batch_scoring,
    dag=dag
)

top_k_upload = PythonOperator(
    task_id='upload_top_k_frauds',
    python_callable=upload_top_k,
    dag=dag
)

create_fraud_table = AthenaOperator(
    task_id="create_fraud_top_k_table",
    query=create_table_sql,
    database="fraud_scoring_db",
    output_location="s3://fraud-batch-pipeline-stevo/query-results/",
    aws_conn_id="aws_default",
    dag=dag,
)

query_fraud_data = AthenaOperator(
    task_id='query_fraud_data',
    query='SELECT * FROM fraud_top_k LIMIT 10;',
    database='fraud_scoring_db',
    output_location='s3://fraud-batch-pipeline-stevo/query-results/',
    aws_conn_id='aws_default',
    dag=dag
)

fraud_alert_branch = BranchPythonOperator(
    task_id='branch_on_fraud_rate',
    python_callable=is_fraud_rate_high,
    dag=dag
)

high_fraud_alert = SlackWebhookOperator(
    task_id='send_high_fraud_alert',
    slack_webhook_conn_id='slack_conn',
    message="🚨 High fraud rate detected in today's batch scoring! Please review.",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag
)

no_alert_needed = PythonOperator(
    task_id='no_alert_needed',
    python_callable=lambda: print('No alert triggered.'),
    dag=dag
)

email_notify = EmailOperator(
    task_id='notify_success',
    to='alerts@risk-team.com',
    subject='[Airflow] ✅ Fraud Batch Scoring Succeeded',
    html_content='Fraud scoring pipeline completed successfully.',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

slack_success = SlackWebhookOperator(
    task_id='slack_success_alert',
    slack_webhook_conn_id='slack_conn',
    message='✅ Fraud scoring DAG completed successfully on {{ ds }}.',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

cleanup = PythonOperator(
    task_id='cleanup_metadata',
    python_callable=cleanup_metadata,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

# DAG workflow
check_input >> run_batch >> top_k_upload >> create_fraud_table >> query_fraud_data >> fraud_alert_branch
fraud_alert_branch >> [high_fraud_alert, no_alert_needed]

high_fraud_alert >> [email_notify, slack_success]
no_alert_needed >> [email_notify, slack_success]

[email_notify, slack_success] >> cleanup

# REGISTER DAG GLOBALLY FOR AIRFLOW
globals()["fraud_scoring_batch_pipeline"] = dag
