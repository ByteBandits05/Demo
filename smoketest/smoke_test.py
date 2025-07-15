# Databricks Notebook

# COMMAND ----------
# This cell prints the start message for the smoke test.
print("Databricks smoke test started.")

# COMMAND ----------
# This cell checks that a Spark session is active and prints the Spark version.
try:
    spark_version = spark.version
    print(f"Active Spark session detected. Spark version: {spark_version}")
except Exception as e:
    raise AssertionError("No active Spark session found. Ensure the notebook is attached to a cluster.") from e

# COMMAND ----------
# This cell creates a small sample DataFrame and displays it.
from pyspark.sql import Row

data = [Row(id=1, value="foo"), Row(id=2, value="bar")]
df = spark.createDataFrame(data)
display(df)  # Databricks notebooks recognize display()

# COMMAND ----------
# This cell asserts that the DataFrame contains the correct number of rows.
expected_rows = 2
actual_rows = df.count()
assert actual_rows == expected_rows, f"Expected {expected_rows} rows, but got {actual_rows}."

# COMMAND ----------
# This cell prints a final success message if all steps have succeeded.
print("Databricks smoke test completed successfully.")
