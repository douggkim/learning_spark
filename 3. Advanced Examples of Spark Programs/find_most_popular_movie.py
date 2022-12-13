from pyspark.sql import SparkSession 
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import os 

spark = SparkSession.builder.appName("PopularMovies").getOrCreate() 

# Create a schema 
schema = StructType([\
    StructField("userID", IntegerType(), True),\
    StructField("movieID", IntegerType(), True),\
    StructField("rating", IntegerType(), True),\
    StructField("timestamp", LongType(), True)]\
    )

file_input = "file://" + os.getcwd() + "/data/ml-100k/u.data"
moviesDF = spark.read.option("sep","\t").schema(schema)\
    .csv(file_input)

topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count")) 

topMovieIDs.show(topMovieIDs.count())

spark.stop()
