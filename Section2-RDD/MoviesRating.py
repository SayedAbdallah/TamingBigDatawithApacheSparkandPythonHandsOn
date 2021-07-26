"""
This Script use RDD API to get how many times each rating occurred in a text file
the file schema is UserId MovieId Rating TimeStamp. each field is separated by space

"""
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("MoviesRating")
sc = SparkContext(conf=conf)

# Every Line contains UserId MovieId Rating TimeStamp
lines = sc.textFile(name="data/ml-100k/u.data")

# map each line to extract only rating Field
ratings = lines.map(lambda line: line.split()[2])
result = ratings.countByValue()

for rating, count in sorted(result.items()):
    print(f"Rating {rating} occurred {count} times")
