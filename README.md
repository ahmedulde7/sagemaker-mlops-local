# PySpark Job for SageMaker Migration

This project contains a PySpark job that can run both locally for testing and in a containerized environment (Docker/Docker Compose). It is designed to run on sagemaker as well.

## Project Structure

```
mlops/
├── jobs/
│   └── data_processing_job.py   # Main PySpark job
├── data/                       # Input/output data directory (mounted in container)
├── logs/                       # Log directory (mounted in container)
├── requirements.txt            # Python dependencies
├── Dockerfile                  # Container build file
├── docker-compose.yml          # Multi-service orchestration
├── migration-sagemaker.md      # Migration plan and notes
├── README.md                   # This file
```

## Features

- **Local and Containerized Execution**: Run the job locally or in Docker
- **Sample Data Generation**: Creates synthetic employee data for testing
- **Data Processing Pipeline**: Includes data cleaning, feature engineering, and aggregations
- **Logging**: Console logging for monitoring and debugging
- **Easy Setup**: Minimal dependencies, containerized for reproducibility

## Prerequisites

- Python 3.8+ (for local runs)
- Java 8+ (required for PySpark, for local runs)
- Docker & Docker Compose (for containerized runs)

## Installation & Usage

### Local Development

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the job locally**:
   ```bash
   python jobs/data_processing_job.py
   ```
   - Output will be saved to `data/output/`.

### Docker (Standalone)

1. **Build the Docker image**:
   ```bash
   docker build -t pyspark-job .
   ```
2. **Run the container**:
   ```bash
   docker run --rm -v $(pwd)/data:/opt/ml/code/data -v $(pwd)/logs:/opt/ml/code/logs pyspark-job
   ```
   - Output will be saved to `data/output/` on your host.

### Docker Compose (Recommended)

1. **Start the services**:
   ```bash
   docker-compose up --build
   ```
   - This will run the PySpark job and mount `data/` and `logs/` for persistence.
   - Spark UI will be available at [localhost:4040](http://localhost:4040) while the job is running.

2. **(Optional) Jupyter Lab**:
   - To use the Jupyter service, run:
     ```bash
     docker-compose --profile jupyter up
     ```
   - Access at [localhost:8889](http://localhost:8889)

## Configuration

- The job script currently uses hardcoded parameters (e.g., 1000 rows of synthetic data).
- Output files:
  - `data/output/processed_data.parquet`
  - `data/output/department_stats.parquet`
  - `data/output/age_stats.parquet`

## Data Processing Pipeline

1. **Data Generation**: Synthetic employee data
2. **RDD Transformations**: Filtering, mapping, reducing, grouping
3. **DataFrame Transformations**: Feature engineering, aggregations
4. **Output**: Parquet files with processed data and statistics

## Troubleshooting

- **Java not found** (local):
  ```bash
  export JAVA_HOME=/path/to/java
  ```
- **Memory issues**:
  - Reduce the number of rows in the script
  - Increase memory allocation in Docker if needed
- **File permissions**:
  - Ensure `data/` and `logs/` are writable by Docker