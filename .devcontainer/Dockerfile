FROM python:3.11-slim

# Set workdir
WORKDIR /workspace

# OS-level dependencies for xmlsec, Airflow, etc.
RUN apt-get update && apt-get install -y \
    gcc \
    libxml2-dev \
    libxmlsec1-dev \
    pkg-config \
    libffi-dev \
    libssl-dev \
    git \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Copy Airflow webserver config
COPY airflow/webserver_config.py /root/airflow/webserver_config.py

# Copy and install requirements
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt
