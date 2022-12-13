from pyspark import SparkConf,SparkContext
import os 


def parseLine(line): 
    fields = line.split(",")
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3])*0.1*(9.0/5.0)+32.0
    return (stationID, entryType, temperature)

# Setup Spark Context 
conf = SparkConf().setMaster("local").setAppName("MinTempByLocation")
sc = SparkContext(conf = conf)

# Load the file 
file_path = "file://" + os.getcwd() + "/1800.csv"
lines = sc.textFile(file_path)
parsedLines = lines.map(parseLine)

# filter min tmp 
minTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
# select the relevant information 
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# Get the minimum temp among temperatures 
# Sort by temperatures ascending
minTemps = stationTemps.reduceByKey(lambda x,y : max(x,y)).sortBy(lambda x: -x[1])

# Print the results 
results = minTemps.collect()
for result in results: 
    print(result[0] + "\t{:.2f}F".format(result[1]))

