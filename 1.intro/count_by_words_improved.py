from pyspark import SparkContext, SparkConf
import os
import re 

def normalizeWords(text): 
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

file_path = "file://" + os.getcwd() + "/Book"
input_text = sc.textFile(file_path)
words = input_text.flatMap(normalizeWords)
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
wordCountsSorted = wordCounts.sortBy(lambda x: x[1])
results = wordCountsSorted.collect() 

# for word, count in wordCounts.items():
#     cleanWord = word.encode("ascii", "ignore")
#     if (cleanWord):
#         print(cleanWord, count)

for result in results: 
    print(f"{result[0]}:\t{result[1]}")