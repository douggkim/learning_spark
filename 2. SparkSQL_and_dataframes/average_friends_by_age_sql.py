from pyspark.sql import SparkSession
import os 

# Create Session 
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# Read File 
file_path = "file://"+os.getcwd() + "/data/fakefriends-header.csv"
people = spark.read.option("header","true").option("inferSchema","true")\
    .csv(file_path)

# get the result 
people.groupBy("age").avg('friends').show()