from pyspark.sql import SparkSession
from pyspark.sql.functions import round, avg

spark = SparkSession.builder.appName("Friends By Age").master("local[*]").getOrCreate()
df = spark.read.option("header", "true").option("inferSchema", "true").csv("../data/fakefriends.csv")

people = df.select("age", "friends")

print('people.groupby("age").avg("friends").show()')
people.groupby("age").avg("friends").show(5)

print('people.groupby("age").avg("friends").show() Sorted')
people.groupby("age").avg("friends").sort("age").show(5)

print("round avg")
people.groupby("age").agg(round(avg("friends"), 2)).sort("age").show(5)

print("alias")
people.groupby("age").agg(round(avg("friends"), 2).alias("avg_friends")).sort("age").show(5)

