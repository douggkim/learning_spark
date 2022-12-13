from pyspark.sql import SparkSession
from pyspark.sql import Row 
import os

# Create a spark session
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# Read File 
file_path = "file://" + os.getcwd() + "/data/fakefriends-header.csv"
people = spark.read.option("header","true").option("inferSchema","true")\
    .csv(file_path)

print("Here is our inferred Schema: ")
people.printSchema() 

# Could use people.select("name").show() instead
print("Let's display the name column: ")
people.select(people.name).show() 

print("Filter out anyone over 21: ")
people.filter(people.age < 21).show()

print("Group by Age : ")
people.groupBy("age").count().show() 

print("Make Everyone 10 yrs older: ")
people.select(people.name, people.age + 10).show() 

spark.stop()
