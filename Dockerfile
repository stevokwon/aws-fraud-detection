FROM python:3.11-slim

# Install OS dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    libpq-dev \
    default-libmysqlclient-dev \
    libsasl2-dev \
    libldap2-dev \
    python3-dev \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    graphviz \
 && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /workspace

# Copy in environment requirements
COPY requirements.txt .

# Pre-install requirements (postCreateCommand will ensure reinstallation in container context)
RUN pip install --upgrade pip && pip install -r requirements.txt
