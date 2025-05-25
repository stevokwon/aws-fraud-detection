# 04_batch_scoring_pipeline.py
# --------------------------------------------
# 🎯 Batch Scoring Script for Fraud Detection
# Load new transactions from AWS S3, score with LightGBM model, and save flagged results back to S3

import os
import logging
from datetime import datetime
import boto3
import pandas as pd
from sklearn.preprocessing import LabelEncoder
import joblib
from io import StringIO

# --------------------------------------------
# Configuration
# --------------------------------------------
S3_BUCKET = 'fraud-batch-pipeline-stevo'
S3_INPUT_KEY = 'incoming/transactions.csv'
S3_OUTPUT_KEY = 'flagged/output_flagged_transactions.csv'
MODEL_PATH = 'models/lightgbm_model.pkl'
FEATURE_PATH = 'models/expected_features.pkl'
THRESHOLD = 0.50 

# --------------------------------------------
# Logging Setup
# --------------------------------------------
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger('fraud_scoring')

def run_batch_scoring():
    run_time = datetime.now().isoformat()
    logger.info(f'Batch scoring job started at {run_time}')

    # --------------------------------------------
    # Load New Data from S3
    # --------------------------------------------
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket = S3_BUCKET, Key = S3_INPUT_KEY)
    df_new = pd.read_csv(obj['Body'])
    logger.info(f'Loaded new transaction data from S3')

    # --------------------------------------------
    # Preprocess the dataset
    # --------------------------------------------
    df_new.columns = [str(col).replace(':', '_').replace(' ', '_').replace('"', '').replace("'", '').replace('[', '').replace(']', '').replace('(', '').replace(')', '') for col in df_new.columns]

    for col in df_new.select_dtypes(include = 'object').columns:
        le = LabelEncoder()
        df_new[col] = le.fit_transform(df_new[col].astype(str))

    columns_to_drop = [col for col in df_new.columns if any(substr in col for substr in ['bin', 'interval', 'bucket']) or df_new[col].dtype == 'object']
    df_new = df_new.drop(columns=columns_to_drop, errors='ignore')

    # --------------------------------------------
    # Align features with the training dataset
    # --------------------------------------------
    expected_features = joblib.load(FEATURE_PATH)
    df_new = df_new.reindex(columns = expected_features, fill_value = 0)
    logger.info(f"🔄 Aligned features to expected model input with shape: {df_new.shape}")

    # --------------------------------------------
    # Load the model and score
    # --------------------------------------------
    model = joblib.load(MODEL_PATH)
    y_probs = model.predict_proba(df_new)[:, 1]
    y_pred = (y_probs >= THRESHOLD).astype(int)

    # --------------------------------------------
    # Save the result to S3
    # --------------------------------------------
    results = df_new.copy()
    results['fraud_probability'] = y_probs
    results['predicted_label'] = y_pred

    csv_buffer = StringIO()
    results.to_csv(csv_buffer, index = False)
    s3.put_object(Bucket = S3_BUCKET, Key = S3_OUTPUT_KEY, Body = csv_buffer.getvalue())
    logger.info(f"✅ Scored results saved to s3://{S3_BUCKET}/{S3_OUTPUT_KEY}")

    # --------------------------------------------
    # Log batch summary
    # --------------------------------------------
    fraud_rate = results['predicted_label'].mean()
    logger.info(f"📊 Fraud Rate: {fraud_rate:.4f}  - Total Records : {len(results)}")

    # Save metadata locally (can be pushed to S3)
    metadata = {
        'run_time': run_time,
        'record_count': len(results),
        'fraud_rate': fraud_rate,
        'model_version': 'v1.0'
    }

    os.makedirs("metadata", exist_ok=True)
    pd.DataFrame([metadata]).to_csv("metadata/scoring_metadata.csv", index=False)
    results.to_csv('metadata/scoring_results.csv', index = False)
    logger.info("📝 Metadata saved to metadata/scoring_metadata.csv")

if __name__ == '__main__':
    run_batch_scoring()
