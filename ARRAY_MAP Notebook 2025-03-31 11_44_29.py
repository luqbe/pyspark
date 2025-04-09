# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder.appName("InsertValues").getOrCreate()

# Define Schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Insert Values
data = [
    (1, "Alice", 23),
    (2, "Bob", 30),
    (3, "Charlie", 25)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col,array_contains

# Initialize Spark Session
spark = SparkSession.builder.appName("ArrayExample").getOrCreate()

# Sample Data
data = [
    (1, "Alice", [10, 20, 30]),
    (2, "Bob", [5, 15]),
    (3, "Charlie", [8, 12, 18, 22])
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "scores"])
df.show(truncate=False)

from pyspark.sql.functions import size,array_min,array_max,array_contains
df1=df.select(col("name"),size("scores").alias("value"),array_contains("scores",10))
df1.show()

# COMMAND ----------

from pyspark.sql.functions import explode
df2=df.select(col("name"),explode(col("scores")))
df2.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, to_json

# Initialize Spark Session
spark = SparkSession.builder.appName("JSON Example").getOrCreate()

# Sample Data
data = [
    (1, "Alice", 25, "New York"),
    (2, "Bob", 30, "San Francisco")
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "age", "city"])

# Convert Struct to JSON String
df_json = df.withColumn("json_data", to_json(struct("name", "age", "city")))
df_json.show(truncate=False)

# COMMAND ----------

