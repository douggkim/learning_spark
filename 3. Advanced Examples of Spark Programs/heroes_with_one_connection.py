from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os 

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate() 

schema = StructType([\
    StructField("id", IntegerType(), True),\
    StructField("name", StringType(), True)]\
    )

names_path = "file://" + os.getcwd() + "/data/Marvel+Names"
relation_path = "file://" + os.getcwd() + "/data/Marvel+Graph"

names = spark.read.schema(schema).option("sep", " ").csv(names_path)
lines = spark.read.text(relation_path)

connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
#Filtering the heros with only one conneciton 
onlyOneConnections  = connections.filter(func.col("connections")==1).select("id") 

#Join names & connections
onlyOneConnNames = names.join(
    onlyOneConnections,
    on="id",
    how="inner"
).select("name")

# for conn in onlyOneConnNames: 
#     print(conn[0] + " only has one connection")

# When using show() don't use collect() before -> turns the datatype to list 
onlyOneConnNames.show()