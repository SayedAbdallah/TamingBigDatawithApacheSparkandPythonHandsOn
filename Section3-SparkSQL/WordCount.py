from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode


spark = SparkSession.builder.master("local").appName("Word Count DataFrame").getOrCreate()

df = spark.read.text("../data/book.txt")
# df.show(5)

splitted = df.select(split(df.value, '\\W+').alias('words'))

exploded = splitted.select(explode(splitted.words).alias('word'))

wordcount = exploded.groupby('word').count().sort('count')

wordcount.show(wordcount.count())

spark.stop()
