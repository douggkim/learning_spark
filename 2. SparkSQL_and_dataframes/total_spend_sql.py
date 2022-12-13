from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
import os

# Create a spark session
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


file_path = "file://" + os.getcwd() + "/data/customer-orders.csv"
customer = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv(file_path)

customer.printSchema()

# With DataFrame
# customer.groupBy("customer_id").sum("purchase_amount").sort("sum(purchase_amount)").show()
customer.groupBy("customer_id").agg(func.round(func.sum("purchase_amount"), 2)\
    .alias("total_spend")).sort("total_spend").show()


# Infer the schema and register the DataFrame as a table 
# cache() : keep in memory
customer.createOrReplaceTempView("customer")

# with SQL
customers = spark.sql("SELECT customer_id, SUM(purchase_amount) AS total_spend FROM customer GROUP BY customer_id ORDER BY total_spend DESC")
for c in customers.collect(): 
    print(c)
