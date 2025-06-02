# Base image
FROM python:3.11-slim

# Install required system libraries
RUN apt-get update && apt-get install -y \
    gcc \
    libxml2-dev \
    libxmlsec1-dev \
    pkg-config \
    libffi-dev \
    libssl-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create user vscode to avoid "no matching entries in passwd file"
RUN useradd -m vscode

# Set working directory
WORKDIR /workspace

# Copy requirements file from local context
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && pip install -r requirements.txt
