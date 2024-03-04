from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import time
import csv

# Initialize Spark session
spark = SparkSession.builder.appName("YearWiseSecurityDelay").getOrCreate()

# Load the dataset
df = spark.read.format("csv").load("s3://flights-delay/dataset/DelayedFlights-updated.csv")

# Save the DataFrame in Parquet format, partitioned by Year (_c1 column)
df.write.format("parquet").partitionBy("_c1").save("s3://flights-delay/spark-output/security_delay/DelayedFlights-updated-df")

# Read the saved Parquet file
df2 = spark.read.format("parquet").load("s3://flights-delay/spark-output/security_delay/DelayedFlights-updated-df")

# Register the DataFrame as a SQL temporary view
df2.createOrReplaceTempView("delay_flights")

# Prepare to capture execution times
execution_times = []

# Run the query 5 times, capturing execution time for each run
for i in range(5):
    start_time = time.time()
    result_df = spark.sql("SELECT _c1 as Year, avg((_c28/_c15)*100) as Year_wise_security_delay from delay_flights GROUP BY _c1 ORDER BY _c1 DESC")
    result_df.collect()  # Force action to ensure timing includes result computation
    end_time = time.time()

    # Calculate execution time
    execution_time = end_time - start_time
    execution_times.append((i, execution_time))
    
    # Save query results to S3, creating a unique path for each iteration
    result_path = f"s3://flights-delay/spark-output/security_delay/iteration_{i}"
    result_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(result_path)
    result_df.unpersist()

# Create a DataFrame for the execution times
schema = ["Iteration", "ExecutionTime"]
execution_times_df = spark.createDataFrame(execution_times, schema=schema)

# Write the execution times DataFrame to S3 as a single CSV file
execution_times_path = "s3://flights-delay/spark-output/security_delay/spark_security_delay_time"
execution_times_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(execution_times_path)

print("All executions completed, results and execution times recorded.")

# Stop Spark session
spark.stop()