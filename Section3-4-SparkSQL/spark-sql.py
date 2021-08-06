from pyspark.sql import SparkSession
from pyspark.sql.types import Row


def mapLine(line: str) -> Row:
    fields = line.split(',')

    return Row(id=int(fields[0]), name=fields[1], age=int(fields[2]), friends=int(fields[3]))


spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

lines = spark.sparkContext.textFile("../data/fakefriends-noheader.csv")
mappedLines = lines.map(mapLine)

# create DataFrame from RDD
df = spark.createDataFrame(data=mappedLines).cache()
df.createOrReplaceTempView("people")

teenagers = spark.sql("select * from people where age between 13 and 19")
for teenager in teenagers.collect():
    print(teenager)


df.groupby('age').count().orderBy('age').show(10)

spark.stop()
