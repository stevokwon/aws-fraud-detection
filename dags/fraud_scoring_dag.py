# fraud_scoring_dag.py
# ---------------------------
# 🎯 Airflow DAG to trigger batch fraud scoring pipeline

from datetime import timedelta
import boto3
import os
import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from scripts.04_batch_scoring_pipeline import run_batch_scoring

# ---------------------------
# Default Configuration
# ---------------------------
default_args = {
    'owner' : 'Fraud Risk Team',
    'depends_on_past' : False,
    'email_on_failure' : True,
    'email' : 'stevekwon0407@gmail.com',
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 10)

}

# ---------------------------
# Check S3 for the input file
# ---------------------------
def check_s3_input_file():
    s3 = boto3.client('s3')
    bucket_name = 'fraud-batch-pipeline-stevo'
    key = 'incoming/transactions.csv'

    try:
        s3.head_object(Bucket = bucket_name, Key = key)
        return True
    except s3.exceptions.ClientError:
        return False

# ------------------------------------
# Conditional alert on high fraud rate
# ------------------------------------
def is_fraud_rate_high():
    metadata_path = '../scripts/metadata/scoring_metadata.csv'
    if os.path.exists(metadata_path):
        df = pd.read_csv(metadata_path)
        return 'send_high_fraud_alert' if df['fraud_rate'].iloc[0] > 0.05 else 'no_need_to_alert'
    return 'no_need_to_alert'

# ---------------------------------
# Upload top-k flagged transactions
# ---------------------------------
def upload_top_k():
    df = pd.read_csv('metadata/scoring_results.csv')
    df_top_k = df.sort_values('fraud_probability', ascending = False).head(100)
    df_top_k.to_csv('metadata/top_k_flagged.csv')
    s3 = boto3.client('s3')
    with open('metadata/top_k_flagged.csv', 'rb') as f:
        s3.upload_fileobj(f, 'fraud-batch-pipeline-stevo', 'review/top_k_flagged.csv')

# ---------------------------------
# Cleanup the local metadata 
# ---------------------------------
def cleanup_metadata():
    files = ['metadata/scoring_metadata.csv', 'metadata/scoring_results.csv', 'metadata/top_k_flagged.csv']
    for file in files:
        if os.path.exists(file):
            os.remove(file)

# ---------------------------
# Define DAG
# ---------------------------
with DAG(
    dag_id = 'fraus_scoring_batch_pipeline',
    default_args = default_args,
    description = 'Run batch scoring on transactions then push results to S3',
    schedule = '@daily',
    start_date = days_ago(1),
    catchup = False,
    tags = ['fraud', 'batch', 'scoring']
) as dag:
    
    check_input = ShortCircuitOperator(
        task_id = 'check_if_input_exists',
        python_callable = check_s3_input_file
    )

    run_batch = PythonOperator(
        task_id = 'run_batch_scoring',
        python_callable = run_batch_scoring
    )

    top_k_upload = PythonOperator(
        task_id = 'upload_top_k_frauds'
        python_callable = upload_top_k
    )

    fraud_alert_branch = BranchPythonOperator(
        task_id = 'branch_on_fraud_rate',
        python_callable = is_fraud_rate_high
    )

    high_fraud_alert = SlackWebhookOperator(
        
    )


