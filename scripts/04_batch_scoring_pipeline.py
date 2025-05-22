# 04_batch_scoring_pipeline.py
# --------------------------------------------
# 🎯 Batch Scoring Script for Fraud Detection
# Load new transactions from AWS S3, score with LightGBM model, and save flagged results back to S3

import os
import logging
import datetime
import boto3
import pandas as pd

# --------------------------------------------
# Configuration
# --------------------------------------------
S3_BUCKET = 'bucket_name'
S3_INPUT_KEY = 'incoming/transactions.csv'
S3_OUTPUT_KEY = 'flagged/output_flagged_transactions.csv'
MODEL_PATH = 'models/lightgbm_model.pkl'
THRESHOLD = 0.50 

# --------------------------------------------
# Logging Setup
# --------------------------------------------
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger('fraud_scoring')
run_time = datetime.utcnow().isoformat()
logger.info(f'Batch scoring job started at {run_time}')

# --------------------------------------------
# Load New Data from S3
# --------------------------------------------
s3 = boto3.client('s3')
obj = s3.get_object(Bucket = S3_BUCKET, Key = S3_INPUT_KEY)
df_new = pd.read_csv(obj['Body'])
logger.info(f'Loaded new transaction data from S3')