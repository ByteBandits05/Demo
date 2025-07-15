# smoketest.py
# Simple smoke test for Databricks job validation

# Import necessary libraries
from pyspark.sql import SparkSession

# Start of the smoke test
print("Smoke test started.")

# Create Spark session (Databricks provides 'spark' by default, so this is only for local testing)
try:
    spark
except NameError:
    spark = SparkSession.builder.getOrCreate()

# Step 1: Create a small sample DataFrame with 3 rows and 2 columns
data = [
    ("Alice", 25),
    ("Bob", 30),
    ("Charlie", 35)
]
columns = ["name", "age"]

df = spark.createDataFrame(data, columns)

# Step 2: Display the DataFrame
# In Databricks, 'display(df)' will show the table nicely
display(df)

# Step 3: Assert the row count is 3
row_count = df.count()
assert row_count == 3, f"Expected 3 rows, but got {row_count}"

print("Row count assertion passed. DataFrame has 3 rows as expected.")

# End of the smoke test
print("Smoke test completed successfully.")
