from pyspark import SparkConf, SparkContext
import os 

# split each comma-delimted line into fields 
def parseLine(line): 
    fields = line.split(',')
    customer_id = int(fields[0])
    amount_spent = float(fields[2])
    return (customer_id, amount_spent)

# setup context 
conf = SparkConf().setMaster("local").setAppName("customerAmountSpent")
sc = SparkContext(conf=conf)

# load file 
file_path = "file://"+ os.getcwd() + "/customer-orders.csv"
lines = sc.textFile(file_path)
# map each line to key/value pairs of customerID and dollar amount 
parsedLines = lines.map(parseLine)

# Use reduceByKey() to sum up 
amountSum  = parsedLines.reduceByKey(lambda x,y : x+y).sortBy(lambda x:-x[1])

# collect and print results
results = amountSum.collect() 
for result in results: 
    print(f"{result[0]} : {result[1]}")