from pyspark.sql import SparkSession 
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys, os 

movie_names_path = "file://" + os.getcwd() + "/data/ml-100k/u.item"
movie_ratings_path = "file://" + os.getcwd() + "/data/ml-100k/u.data"

def computeCosineSimilarity(spark, data): 
    # compute xx, xy, and yy columns 
    pairScores = data\
        .withColumn("xx", func.col("rating1") * func.col("rating1"))\
        .withColumn("yy", func.col("rating2") * func.col("rating2"))\
        .withColumn("xy", func.col("rating1") * func.col("rating2"))
    
    # Compute numerator, denominator and numPairs Columns 
    calculateSimilarity = pairScores\
        .groupBy("movie1", "movie2")\
        .agg(\
            func.sum(func.col("xy")).alias("numerator"),\
            (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"),\
            func.count(func.col("xy")).alias("numPairs")
        )
    
    #calculate score and select only needed columns (movie1, movie2, score, numPairs)
    result = calculateSimilarity\
        .withColumn("score",\
        func.when(func.col("denominator") != 0, func.col("numerator")/func.col("denominator"))\
        .otherwise(0)\
        ).select("movie1", "movie2", "score", "numPairs")
    
    return result 

#Get Movie name by given movie id 
def getMovieName(movieNames, movieId): 
    result = movieNames.filter(func.col("movieID") == movieId)\
    .select("movieTitle").collect()[0]

    return result[0]

# "local[*]" use everycore possible
spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate() 

movieNamesSchema = StructType([\
    StructField("movieID", IntegerType(), True),
    StructField("movieTitle", StringType(), True)
    ]
)

moviesSchema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# Create a broadcast dataset of movieID and movieTitle 
# Apply ISO-885901 charset 

movieNames = spark.read.option("sep","|").option("charset","ISO-8859-1").schema(movieNamesSchema).csv(movie_names_path)
movies = spark.read.option("sep","\t").schema(moviesSchema).csv(movie_ratings_path)

ratings = movies.select("userID", "movieID", "rating")

# Emit every movie rated together by the same user 
# Self-join to find every combination 
# Select movie Pairs and rating pairs 
moviePairs = ratings.alias("ratings1")\
    .join(ratings.alias("ratings2"), (func.col("ratings1.userID")==func.col("ratings2.userID"))\
        & (func.col("ratings1.movieID")< func.col("ratings2.movieID")))\
    .select(func.col("ratings1.movieID").alias("movie1"),\
        func.col("ratings2.movieID").alias("movie2"),\
        func.col("ratings1.rating").alias("rating1"),\
        func.col("ratings2.rating").alias("rating2"))

moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache() 

if (len(sys.argv) > 1): 
    scoreThreshold = 0.97 
    coOccurrenceThreshold = 50.0 

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by 
    # our quality thresholds above 

    filteredResults = moviePairSimilarities.filter(
        ((func.col("movie1") == movieID) | (func.col("movie2")==movieID)) &
        ((func.col("score")>scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))
    )

    results = filteredResults.sort(func.col("score").desc()).take(10)

    print("Top 10 similar movies for " + getMovieName(movieNames, movieID))

    for result in results: 
        # Display the similarity results that isn't the movie we're looking at
        similarMovieID = result.movie1 
        if (similarMovieID == movieID): 
            similarMovieID = result.movie2
        
        print(getMovieName(movieNames, similarMovieID) +"\tscore:"\
        +str(result.score) + "\tstrength " + str(result.numPairs))