# Databricks notebook source
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable


# Create sample customer data
customer_data = [
    (1, "John Doe", "john.doe@email.com", "2023-01-15"),
    (2, "Jane Smith", "jane.smith@email.com", "2023-02-20"),
    (3, "Bob Johnson", "bob.johnson@email.com", "2023-03-10"),
    (4, "Alice Brown", "alice.brown@email.com", "2023-04-05"),
    (5, "Charlie Wilson", "charlie.wilson@email.com", "2023-05-12")
]

customer_df = spark.createDataFrame(
    customer_data, 
    ["customer_id", "name", "email", "registration_date"]
)

print("Sample customer data created")
customer_df.show()

# Simulate data processing delay
print("Processing data ingestion...")
time.sleep(5)
print("Data ingestion completed successfully!")

# Write data to Delta table
customer_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "/tmp/delta/customers") \
    .saveAsTable("customers")

print("Customer data written to Delta table successfully")
