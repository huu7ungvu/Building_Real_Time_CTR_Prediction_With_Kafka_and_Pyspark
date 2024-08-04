from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("SparkClusterTest") \
    .getOrCreate()

# Sample data for testing
data = [("Alice", 29), ("Bob", 31), ("Cathy", 25)]
columns = ["Name", "Age"]

# Create a DataFrame from the sample data
df = spark.createDataFrame(data, columns)

# Show the DataFrame content
print("DataFrame Content:")
df.show()

# Perform a simple operation: count the number of rows
count = df.count()
print(f"Number of rows in the DataFrame: {count}")

# Save DataFrame to a CSV file (optional)
df.write.csv("/opt/spark/data/results/output.csv", header=True)

# Stop the Spark session
spark.stop()
