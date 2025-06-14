# Run the pipeline in an isolated container
FROM python:3.11-slim

ENV PYSPARK_PYTHON=python3
ENV PYTHONUNBUFFERED=1

# Install Java for Spark
RUN apt-get update \
    && apt-get install -y openjdk-11-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Default command executes the pipeline (local Spark Master)
CMD ["spark-submit", "src/pipeline.py"]
