#!/usr/bin/env python3
"""
Simple PySpark Job - RDD Transformations
A basic PySpark job that creates RDDs and applies transformations.
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, sum as spark_sum

def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def create_spark_session():
    """Create and configure Spark session."""
    spark = SparkSession.builder \
        .appName("SimplePySparkJob") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    
    # Disable parquet job summary to avoid known Spark bug
    spark.conf.set("spark.sql.parquet.mergeSchema", "false")
    
    return spark

def create_sample_data(spark, num_rows=1000):
    """Create sample data using RDD and convert to DataFrame."""
    logger = logging.getLogger(__name__)
    
    # Create sample data as RDD
    import random
    
    # Sample data structure: (id, name, age, salary, department)
    departments = ["Engineering", "Sales", "Marketing", "HR", "Finance"]
    names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
    
    def generate_record():
        return (
            random.randint(1, 10000),
            random.choice(names),
            random.randint(22, 65),
            random.randint(30000, 150000),
            random.choice(departments)
        )
    
    # Create RDD with sample data
    logger.info(f"Creating RDD with {num_rows} sample records...")
    rdd = spark.sparkContext.parallelize([generate_record() for _ in range(num_rows)])
    
    # Convert RDD to DataFrame
    df = rdd.toDF(["id", "name", "age", "salary", "department"])
    
    logger.info(f"Created DataFrame with {df.count()} records")
    df.printSchema()
    
    return df

def apply_rdd_transformations(spark, df):
    """Apply various RDD transformations."""
    logger = logging.getLogger(__name__)
    
    # Convert DataFrame back to RDD for transformations
    logger.info("Converting DataFrame to RDD for transformations...")
    
    # 1. Filter RDD - only employees with salary > 50000
    high_salary_rdd = df.rdd.filter(lambda row: row.salary > 50000)
    logger.info(f"High salary employees (>50k): {high_salary_rdd.count()}")
    
    # 2. Map transformation - create (department, salary) pairs
    dept_salary_rdd = df.rdd.map(lambda row: (row.department, row.salary))
    logger.info("Created department-salary pairs")
    
    # 3. Reduce by key - calculate average salary by department
    dept_avg_salary = dept_salary_rdd.mapValues(lambda salary: (salary, 1)) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .mapValues(lambda x: x[0] / x[1])
    
    logger.info("Calculated average salary by department:")
    for dept, avg_salary in dept_avg_salary.collect():
        logger.info(f"  {dept}: ${avg_salary:.2f}")
    
    # 4. Group by transformation - group by age ranges
    age_groups_rdd = df.rdd.map(lambda row: (
        "Young" if row.age < 30 else "Middle" if row.age < 50 else "Senior",
        row.salary
    ))
    
    age_group_stats = age_groups_rdd.mapValues(lambda salary: (salary, 1)) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .mapValues(lambda x: (x[0] / x[1], x[1]))  # (avg_salary, count)
    
    logger.info("Age group statistics:")
    for age_group, (avg_salary, count) in age_group_stats.collect():
        logger.info(f"  {age_group}: {count} employees, avg salary: ${avg_salary:.2f}")
    
    return dept_avg_salary, age_group_stats

def apply_dataframe_transformations(df):
    """Apply DataFrame transformations."""
    logger = logging.getLogger(__name__)
    
    logger.info("Applying DataFrame transformations...")
    
    # 1. Add new columns
    df_with_features = df.withColumn(
        "age_group",
        when(col("age") < 30, "Young")
        .when(col("age") < 50, "Middle")
        .otherwise("Senior")
    ).withColumn(
        "salary_category",
        when(col("salary") < 50000, "Low")
        .when(col("salary") < 100000, "Medium")
        .otherwise("High")
    )
    
    # 2. Aggregations
    dept_stats = df_with_features.groupBy("department").agg(
        count("*").alias("employee_count"),
        avg("salary").alias("avg_salary"),
        avg("age").alias("avg_age")
    )
    
    age_stats = df_with_features.groupBy("age_group").agg(
        count("*").alias("count"),
        avg("salary").alias("avg_salary")
    )
    
    logger.info("Department statistics:")
    dept_stats.show()
    
    logger.info("Age group statistics:")
    age_stats.show()
    
    return df_with_features, dept_stats, age_stats

def save_results(spark, df_processed, dept_stats, age_stats):
    """Save results to data directory."""
    logger = logging.getLogger(__name__)
    
    # Create output directory
    output_dir = "/opt/ml/code/data/output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Save processed data
    processed_path = os.path.join(output_dir, "processed_data.parquet")
    df_processed.write.mode("overwrite").parquet(processed_path)
    logger.info(f"Saved processed data to: {processed_path}")
    
    # Save department statistics
    dept_stats_path = os.path.join(output_dir, "department_stats.parquet")
    dept_stats.write.mode("overwrite").parquet(dept_stats_path)
    logger.info(f"Saved department stats to: {dept_stats_path}")
    
    # Save age statistics
    age_stats_path = os.path.join(output_dir, "age_stats.parquet")
    age_stats.write.mode("overwrite").parquet(age_stats_path)
    logger.info(f"Saved age stats to: {age_stats_path}")

def main():
    """Main execution function."""
    logger = setup_logging()
    logger.info("Starting Simple PySpark Job...")
    
    try:
        # Create Spark session
        spark = create_spark_session()
        logger.info("Spark session created successfully")
        
        # Create sample data
        df = create_sample_data(spark, num_rows=1000)
        
        # Apply RDD transformations
        dept_avg_salary, age_group_stats = apply_rdd_transformations(spark, df)
        
        # Apply DataFrame transformations
        df_processed, dept_stats, age_stats = apply_dataframe_transformations(df)
        
        # Save results
        save_results(spark, df_processed, dept_stats, age_stats)
        
        logger.info("PySpark job completed successfully!")
        
        # Show some sample data
        logger.info("Sample of processed data:")
        df_processed.show(5)
        
    except Exception as e:
        logger.error(f"Error in PySpark job: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main() 