from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext(master="local[*]", appName="Test")

spark = SparkSession\
    .builder\
    .appName(name="Test")\
    .master(master="local[*]")\
    .getOrCreate()


lines = sc.textFile(name="data/ml-100k/README")
count = lines.count()

print(f"There are {count} Lines in the File")
