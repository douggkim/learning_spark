from pyspark.sql import SparkSession 
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import os 
import codecs 

def loadMovieNames(): 
    movieNames = {} 
    movie_file_path = os.getcwd() + "/data/ml-100k/u.ITEM"

    with codecs.open(movie_file_path, "r", encoding = "ISO-8859-1", errors="ignore") as f: 
        for line in f: 
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
        return movieNames 

spark = SparkSession.builder.appName("PopularMovies").getOrCreate() 

nameDict = spark.sparkContext.broadcast(loadMovieNames())

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

movieCounts = moviesDF.groupBy("movieID").count()

def lookupName(movieID): 
    return nameDict.value[movieID]

lookupNameUDF = func.udf(lookupName)

movieWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

sortedMoviesWithNames = movieWithNames.orderBy(func.desc("count"))

sortedMoviesWithNames.show(10, False)

spark.stop()
