"""
in this demo we will try
BroadCast Variables and UDF
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType
from pyspark.sql.functions import udf, col, desc


def loadMovies():
    namesDict = {}
    with open(file="../data/ml-100k/u.item", encoding="ISO-8859-1") as f:
        for line in f:
            fields = line.split('|')
            movieId = int(fields[0])
            movieName = fields[1]
            namesDict[movieId] = movieName

    return namesDict


schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])


def startDemo():

    # create SparkSession object
    spark = SparkSession.builder.appName("Most Popular Movies").master("local").getOrCreate()

    # BroadCast the Dictionary that contains Id->Name
    namesDict = spark.sparkContext.broadcast(loadMovies())

    # create a UDF to get name by id
    nameUDF = udf(lambda movie_id: namesDict.value[movie_id], StringType())

    # read u.data file and get count by each movie id
    df = spark.read.option("sep", "\t").schema(schema).csv(path="../data/ml-100k/u.data")
    df = df.groupby("movieId").count()
    df = df.withColumn("movieName", nameUDF(col("movieId")))
    df = df.sort(desc(col("count")))
    df.show(10)


if __name__ == "__main__":
    startDemo()
