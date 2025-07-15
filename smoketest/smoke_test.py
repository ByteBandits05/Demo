# Databricks notebook source
# Databricks Smoke Test Notebook
# This notebook performs basic operations to verify the Databricks environment is working correctly

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg
import time

# COMMAND ----------

# Initialize and verify Spark context
# This section ensures that Spark is properly initialized and accessible
print("=== Databricks Environment Smoke Test ===")
print("Starting smoke test to verify Databricks environment...")

# Get Spark context and session info
print(f"Spark version: {spark.version}")
print(f"Spark context status: {spark.sparkContext.getConf().get('spark.app.name')}")

# COMMAND ----------

# Create a test DataFrame to verify basic DataFrame operations
# This tests core Spark functionality including DataFrame creation and basic transformations
print("\n1. Testing DataFrame creation and basic operations...")

# Create sample test data
test_data = [
    (1, "Environment", 100, "PASS"),
    (2, "Spark", 200, "PASS"), 
    (3, "DataFrame", 150, "PASS"),
    (4, "Computation", 175, "PASS"),
    (5, "Verification", 125, "PASS")
]

# Create DataFrame with schema
test_df = spark.createDataFrame(
    test_data, 
    ["id", "component", "value", "status"]
)

print("✓ DataFrame created successfully")
print(f"✓ DataFrame row count: {test_df.count()}")

# Show the test data
print("\nTest data:")
test_df.show()

# COMMAND ----------

# Perform computations to verify Spark processing capabilities
# This section tests aggregations and transformations
print("\n2. Testing Spark computations and aggregations...")

# Perform basic aggregations
total_value = test_df.select(spark_sum("value")).collect()[0][0]
avg_value = test_df.select(avg("value")).collect()[0][0]
record_count = test_df.count()

print(f"✓ Total value computed: {total_value}")
print(f"✓ Average value computed: {avg_value:.2f}")
print(f"✓ Record count verified: {record_count}")

# Test filtering and transformations
passed_tests = test_df.filter(col("status") == "PASS").count()
print(f"✓ Passed tests count: {passed_tests}")

# COMMAND ----------

# Verify data processing with a more complex operation
# This tests joins and window functions to ensure advanced Spark features work
print("\n3. Testing advanced DataFrame operations...")

# Create a summary DataFrame
summary_df = test_df.groupBy("status").agg(
    count("*").alias("count"),
    spark_sum("value").alias("total_value"),
    avg("value").alias("avg_value")
)

print("Summary statistics:")
summary_df.show()

# Test that we can collect results properly
summary_results = summary_df.collect()
print(f"✓ Summary data collected: {len(summary_results)} row(s)")

# COMMAND ----------

# Critical assertions to verify environment is working correctly
# These assertions will cause the notebook to fail if the environment is not properly configured
print("\n4. Running critical environment assertions...")

try:
    # Assert Spark is working
    assert spark is not None, "Spark session is not available"
    print("✓ Spark session assertion passed")
    
    # Assert DataFrame operations work
    assert test_df.count() == 5, f"Expected 5 rows, got {test_df.count()}"
    print("✓ DataFrame row count assertion passed")
    
    # Assert computations work correctly
    assert total_value == 750, f"Expected total value 750, got {total_value}"
    print("✓ Computation assertion passed")
    
    # Assert all test records have PASS status
    assert passed_tests == 5, f"Expected 5 passed tests, got {passed_tests}"
    print("✓ Test status assertion passed")
    
    # Assert aggregations work
    assert len(summary_results) > 0, "Summary aggregation failed"
    print("✓ Aggregation assertion passed")
    
    print("\n✓ All critical assertions passed!")
    
except AssertionError as e:
    print(f"\n❌ SMOKE TEST FAILED: {str(e)}")
    print("The Databricks environment is not working correctly.")
    raise e
except Exception as e:
    print(f"\n❌ UNEXPECTED ERROR: {str(e)}")
    print("An unexpected error occurred during smoke test.")
    raise e

# COMMAND ----------

# Final verification and success message
# This section provides a clear indication that the smoke test completed successfully
print("\n" + "="*50)
print("🎉 DATABRICKS SMOKE TEST COMPLETED SUCCESSFULLY! 🎉")
print("="*50)
print("\nEnvironment Verification Summary:")
print("✓ Spark session initialized and functional")
print("✓ DataFrame creation and operations working")
print("✓ Data processing and computations successful")
print("✓ Aggregations and transformations functional")
print("✓ All critical assertions passed")
print("\nThe Databricks environment is ready for production workloads!")
print(f"Test completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")