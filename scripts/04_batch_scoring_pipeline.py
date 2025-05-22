# 04_batch_scoring_pipeline.py
# --------------------------------------------
# 🎯 Batch Scoring Script for Fraud Detection
# Load new transactions from AWS S3, score with LightGBM model, and save flagged results back to S3

import os

# --------------------------------------------
# Configuration
# --------------------------------------------
S3_BUCKET = 'bucket_name'
S3_INPUT_KEY = 'incoming/transactions.csv'
S3_OUTPUT_KEY = 'flagged/output_flagged_transactions.csv'

# --------------------------------------------
# Ingestion
# --------------------------------------------

# --------------------------------------------
# 
# --------------------------------------------
