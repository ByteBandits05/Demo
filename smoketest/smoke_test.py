# Databricks Smoke Test Notebook
# This script performs a minimal smoke test to validate that the Databricks environment is working.

# Cell 1: Import required modules and check Spark session
# (In Databricks, 'spark' session is auto-available)
print("=== Databricks Smoke Test Start ===")

try:
    # Check if Spark session is available and print its version
    spark_version = spark.version
    print(f"Succeeded: Spark session is active (version: {spark_version})")
except Exception as e:
    raise AssertionError(f"ERROR: Spark session is not available: {e}")

# Cell 2: Create and display a simple DataFrame
try:
    # Create a DataFrame with sample data
    data = [(1, "alpha"), (2, "beta")]
    df = spark.createDataFrame(data, ["id", "label"])
    print("Succeeded: DataFrame created")
    df.show()
except Exception as e:
    raise AssertionError(f"ERROR: DataFrame creation/display failed: {e}")

# Cell 3: Assert DataFrame contents
try:
    count = df.count()
    assert count == 2, f"ERROR: DataFrame should have 2 rows, found {count}"
    print("Succeeded: DataFrame row count assertion passed")
except Exception as e:
    raise AssertionError(f"ERROR: DataFrame assertion failed: {e}")

# Cell 4: Print final success message
print("\n=== SUCCESS: Databricks environment smoke test passed ===")
