from __future__ import print_function 
import os 

from pyspark.ml.regression import LinearRegression

from pyspark.sql import SparkSession 
from pyspark.ml.linalg import Vectors

if __name__ == "__main__": 

    # create a SparkSession (note, the config section is only for windows 
    spark = SparkSession.builder.appName("LinearRegression").getOrCreate()

    # Load up our data and convert it to the format MLLib expects 
    txt_path = os.getcwd() + "/data/regression.txt"
    inputLines = spark.sparkContext.textFile(txt_path)
    data = inputLines.map(lambda x:x.split(",")).map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))

    # Convert this RDD to dataFrame 
    colNames = ["label", "features"]
    df = data.toDF(colNames)

    # Note, there are lots of cases where you can avoid going from an RDD to DF 
    # Perhaps you're importing data from a real DB. or you're using structured streaming 
    # to get your data 

    # Let's split our data into training data and testing data 
    trainTest = df.randomSplit([0.5,0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # Now create our linear regression model 
    lir = LinearRegression(maxIter = 10, regParam = 0.3, elasticNetParam=0.8)

    # Train the model using our training data 
    model = lir.fit(trainingDF)

    #Now See if we can predict values in our test data 
    # Generate predictions using our linear regression model for all features in our 
    # test DF : 
    fullPredictions = model.transform(testDF).cache() 

    # Extract the predictions and the correct labels 
    predictions = fullPredictions.select("prediction").rdd.map(lambda x:x[0])
    labels = fullPredictions.select("label").rdd.map(lambda x:x[0])
    
    #Zip them together 
    predictionAndLabel = predictions.zip(labels).collect()

    # print out the predicted and actual values for each point 
    for pred in predictionAndLabel: 
        print(pred)

    # Stop the session 
    spark.stop() 
