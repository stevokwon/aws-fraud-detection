# fraud_scoring_dag.py
# ---------------------------
# 🎯 Airflow DAG to trigger batch fraud scoring pipeline

from airflow import DAG
from datetime import timedelta

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