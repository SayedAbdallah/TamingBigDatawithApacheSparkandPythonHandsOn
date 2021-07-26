"""
Calculation Average Number of Friends By Age
the file contains the schema id, name, age, numberOfFreinds
we load the file then map it to Key/Value RDD of form (age, (numberOfFriends, 1))
after that we reduce it by key to get the result (age, (totalNumberOfFriends, TotalOccurrence))
then we map it to get the final result (age, totalNumberOfFriends/TotalOccurrence )
"""
from typing import Tuple

from pyspark import SparkConf, SparkContext


def parseLine(line:str) -> Tuple:
    fields = line.split(",")
    age = int(fields[2])
    friends = int(fields[3])
    return age, (friends, 1)


# Create SparkContext Object
sc = SparkContext(master="local[*]", appName="Average Number of Friends By Age")

# read the file as Rdd of strings, every row is a RDD
lines = sc.textFile(name="data/fakefriends-noheader.csv")

# map each line to tuple of (age, (friends, 1))
friendsRdd = lines.map(parseLine)

# reduce by key, age, to calculate the total number of friends for each age and total number of occurrence
friendsTotalRdd = friendsRdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# map each element to calculate avg
result = friendsTotalRdd.mapValues(lambda t: t[0] / t[1]).sortByKey().collect()

for r in result:
    print(f"Avg number of friends for age {r[0]} is {r[1]:.2f}")
