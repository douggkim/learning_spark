from pyspark import SparkConf, SparkContext
import collections
import os 

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

file_path = "file://"+ os.getcwd() + "/ml-100k/u.data"
lines = sc.textFile(file_path)
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))

for key, value in sortedResults.items():
    print("%s %i" % (key, value))
