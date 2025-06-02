FROM python:3.11-slim

# Install required system packages
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    g++ \
    libpq-dev \
    libxml2-dev \
    libxmlsec1-dev \
    xmlsec1 \
    libssl-dev \
    libffi-dev \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /workspaces/aws-fraud-detection
