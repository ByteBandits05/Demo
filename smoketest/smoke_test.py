# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Environment Smoke Test
# MAGIC 
# MAGIC This notebook performs basic validation of the Databricks environment to ensure
# MAGIC that Spark and core functionality are working correctly.

# COMMAND ----------

# Cell 1: Import required libraries and initialize Spark session
# This validates that PySpark is available and can be imported
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count as spark_count
import sys

print("✓ Successfully imported PySpark libraries")
print(f"Python version: {sys.version}")

# COMMAND ----------

# Cell 2: Get Spark session and validate it's working
# This ensures the Spark context is available and functioning
spark = SparkSession.getActiveSession()
if spark is None:
    spark = SparkSession.builder.appName("SmokeTest").getOrCreate()

print(f"✓ Spark session created successfully")
print(f"Spark version: {spark.version}")
print(f"Application ID: {spark.sparkContext.applicationId}")

# COMMAND ----------

# Cell 3: Create a simple test DataFrame
# This validates that Spark can create and manipulate DataFrames
test_data = [
    (1, "Alice", 100),
    (2, "Bob", 200),
    (3, "Charlie", 300),
    (4, "Diana", 400),
    (5, "Eve", 500)
]

df = spark.createDataFrame(test_data, ["id", "name", "value"])

print("✓ Test DataFrame created successfully")
df.show()

# COMMAND ----------

# Cell 4: Perform basic DataFrame operations
# This tests core Spark functionality including transformations and actions
row_count = df.count()
total_value = df.select(spark_sum("value").alias("total")).collect()[0]["total"]
max_value = df.select("value").orderBy(col("value").desc()).first()["value"]

print(f"✓ DataFrame operations completed:")
print(f"  - Row count: {row_count}")
print(f"  - Total value: {total_value}")
print(f"  - Maximum value: {max_value}")

# COMMAND ----------

# Cell 5: Perform assertions to validate environment
# These assertions will fail if the environment is not working correctly
try:
    # Assert basic DataFrame functionality
    assert row_count == 5, f"Expected 5 rows, but got {row_count}"
    assert total_value == 1500, f"Expected total value of 1500, but got {total_value}"
    assert max_value == 500, f"Expected max value of 500, but got {max_value}"
    
    # Assert Spark session is active
    assert spark is not None, "Spark session is not available"
    assert spark.sparkContext is not None, "Spark context is not available"
    
    print("✓ All assertions passed successfully")
    
except AssertionError as e:
    print(f"❌ Assertion failed: {e}")
    raise
except Exception as e:
    print(f"❌ Unexpected error during validation: {e}")
    raise

# COMMAND ----------

# Cell 6: Final validation and success message
# This provides a clear indication that the smoke test completed successfully
print("\n" + "="*60)
print("🎉 DATABRICKS ENVIRONMENT SMOKE TEST COMPLETED SUCCESSFULLY!")
print("="*60)
print("✓ PySpark libraries imported successfully")
print("✓ Spark session created and configured")
print("✓ DataFrame operations working correctly")
print("✓ Data transformations and actions functional")
print("✓ All assertions passed")
print("\nThe Databricks environment is ready for use!")
print("="*60)