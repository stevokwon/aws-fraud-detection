import boto3
import pandas as pd

# Configure the necessary constants
bucket_name = 'fraud-batch-pipeline-stevo'
input_key = 'incoming/transactions.csv'
local_path = 'data/raw/sample_transactions.csv'

# Sample the local dataset
df = pd.read_csv('data/raw/creditcard.csv')
df_sample = df.drop(columns = ['Class'])
df_sample.to_csv(local_path, index = False)

# Upload the file to S3
s3 = boto3.client('s3')
with open(local_path, 'rb') as f:
    s3.upload_fileobj(f, bucket_name, input_key)

print(f'✅ Uploaded sample test set to s3://{bucket_name}/{input_key}')