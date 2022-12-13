from pyspark import SparkConf, SparkContext
import collections
import os 

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age,numFriends)

# Create Spark Context 
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

# Load Data 
file_path = "file://"+ os.getcwd() + "/fakefriends.csv"
lines = sc.textFile(file_path)

# Parse Data 
rdd = lines.map(parseLine)

#for count : rdd.mapValues(lambda x: (x,1))
#Sum up  : reduceByKey(lambda x,y : x[0]+y[0], x[1]+y[1]) -> nothing happens until this point
# need x, y when manipulating multiple rows 
totalsByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y:(x[0] + y[0], x[1]+y[1]))
# sort by key : sort in ascending order of results 
averagesByAge = totalsByAge.mapValues(lambda x: x[0]/ x[1]).sortByKey()

#collect and print
results = averagesByAge.collect() 
for result in results: 
    print(result)



