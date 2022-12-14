from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, IntegerType, LongType 
from pyspark.ml.recommendation import ALS 
import sys, os 
import codecs 

def loadMovieNames():
    movieNames = {}
    movie_path = os.getcwd() + "/data/ml-100k/u.item"
    # CHANGE THIS TO THE PATH TO YOUR u.ITEM FILE:
    with codecs.open(movie_path, "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("ALSExample").getOrCreate()

moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

names = loadMovieNames()
ratings_path = "file://" + os.getcwd() + "/data/ml-100k/u.data"
ratings = spark.read.option("sep","\t").schema(moviesSchema)\
    .csv(ratings_path)

print("Training Recommendation Model...")

als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userID").setItemCol("movieID")\
    .setRatingCol("rating")

model = als.fit(ratings)

# Manually Construct a dataframe of the user ID's we want recs for
userID = int(sys.argv[1])
userSchema = StructType(
    [StructField("userID", IntegerType(), True)]
)
users = spark.createDataFrame([[userID,]], userSchema)

recommendations = model.recommendForUserSubset(users, 10).collect()

print("Top 10 recommendations for user ID" + str(userID))

for userRecs in recommendations: 
    myRecs = userRecs[1] #userRecs is (userID, [Row(movieID, rating), Row(movieID, rating)...])
    for rec in myRecs: #my Recs is just the column of recs of the user
        movie = rec[0] # For each rec in the list, extract the movie ID and rating 
        rating = rec[1]
        movieName = names[movie]
        print(movieName + str(rating))