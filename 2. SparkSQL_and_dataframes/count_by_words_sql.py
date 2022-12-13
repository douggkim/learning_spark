from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
import os 


spark = SparkSession.builder.appName("WordCount").getOrCreate()

#Read file 
file_path = "file://" + os.getcwd() + "/data/book.txt"
inputDF = spark.read.text(file_path)

# Split the row based on non-alphabetical and _ character
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")
words.filter(words.word != " ")

lowerCaseWords = words.select(func.lower(words.word).alias("word"))

wordCounts = lowerCaseWords.groupBy("word").count()

wordCountsSorted = wordCounts.sort("count")
# for show() pass in a parameter to decide how many rows you will show (default : 20)
wordCountsSorted.show(wordCountsSorted.count())