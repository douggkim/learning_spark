from __future__ import print_function 
import os 

from pyspark.ml.regression import DecisionTreeRegressor

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

if __name__ == "__main__": 

    spark = SparkSession.builder.appName("DT").getOrCreate() 

    data_path = "file://" + os.getcwd() + "/data/realestate.csv"
    # Read File 
    estateDF = spark.read.option("header","true").option("inferSchema","true")\
        .csv(data_path)
    
    estateDF = estateDF.withColumnRenamed("PriceOfUnitArea","label")
    assembler = VectorAssembler(outputCol="features").setInputCols(["HouseAge", "DistanceToMRT", "NumberConvenienceStores"])
    df = assembler.transform(estateDF).select("features","label")

    trainTest = df.randomSplit([0.5,0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # Could just do this : dtr = DecisionTreeRegressor().setFeaturesCol("features").setLabelCol("PriceOfUnitArea")
    dtr = DecisionTreeRegressor()
    
    model = dtr.fit(trainingDF)

    fullPreds = model.transform(testDF).cache()
    preds = fullPreds.select("prediction").rdd.map(lambda x:x[0])
    labels = fullPreds.select("label").rdd.map(lambda x:x[0])
    
    #Zip them together 
    predictionAndLabel = preds.zip(labels).collect()

    # print out the predicted and actual values for each point 
    for pred in predictionAndLabel: 
        print(pred)

    # Stop the session 
    spark.stop() 



    


    # predict the price per unit area based on house age, distance to MRT 
    # (Public Transportation) and number of nearby convenience stores 