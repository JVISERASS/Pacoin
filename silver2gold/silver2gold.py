from pyspark.sql import SparkSession

# SparkSession
spark = SparkSession.builder.appName("Silver").getOrCreate()

