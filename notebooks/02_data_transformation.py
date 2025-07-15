# Databricks notebook source

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("Libraries imported successfully")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataTransformation") \
    .getOrCreate()

print("Spark session initialized for data transformation")

# Create sample sales data
sales_data = [
    (1, 1, "Product A", 100.50, "2023-06-01"),
    (2, 2, "Product B", 75.25, "2023-06-02"),
    (3, 1, "Product C", 200.00, "2023-06-03"),
    (4, 3, "Product A", 100.50, "2023-06-04"),
    (5, 4, "Product D", 150.75, "2023-06-05")
]

sales_df = spark.createDataFrame(
    sales_data,
    ["transaction_id", "customer_id", "product_name", "amount", "transaction_date"]
)

print("Sample sales data created")
sales_df.show()

# Perform data transformations
transformed_df = sales_df \
    .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")) \
    .withColumn("year", year(col("transaction_date"))) \
    .withColumn("month", month(col("transaction_date"))) \
    .withColumn("amount_rounded", round(col("amount"), 2))

print("Data transformation applied")
transformed_df.show()

# Simulate transformation processing delay
print("Processing data transformations...")
time.sleep(5)
print("Data transformation completed successfully!")

# Aggregate sales by product
product_summary = transformed_df.groupBy("product_name") \
    .agg(
        sum("amount").alias("total_sales"),
        count("transaction_id").alias("transaction_count"),
        avg("amount").alias("avg_amount")
    ) \
    .orderBy(desc("total_sales"))

print("Product sales summary:")
product_summary.show()
