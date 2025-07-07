# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set environment variables
ENV PYTHONPATH=/opt/ml/code
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    wget \
    unzip \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Install Apache Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN wget -qO- https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar zx -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create application directory
WORKDIR /opt/ml/code

# Copy application code
COPY . /opt/ml/code/

# Create data directories
RUN mkdir -p /opt/ml/code/data/input \
    && mkdir -p /opt/ml/code/data/output \
    && mkdir -p /opt/ml/code/data/temp

# Set permissions for the job file
RUN chmod +x /opt/ml/code/jobs/data_processing_job.py

# Expose ports for Spark UI
EXPOSE 4040

# Default command
CMD ["python", "jobs/data_processing_job.py"] 