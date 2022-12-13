from pyspark import SparkContext, SparkConf
import os 

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

file_path = "file://" + os.getcwd() + "/Book"
input_text = sc.textFile(file_path)
words = input_text.flatMap(lambda x:x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode("ascii", "ignore")
    if (cleanWord):
        print(cleanWord, count)