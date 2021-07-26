from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat

spark = SparkSession.builder.appName("DataFrame Demo").master("local[*]").getOrCreate()
df = spark.read.option("header", "true").option("inferSchema", "true").csv("../data/fakefriends.csv")

print("Data Frame")
df.show(5)

print("select name column only")
df.select("name").show(5)

print("filter out age above 21")
df.filter(df.age < 21).show(5)

print("count by age")
df.groupby("age").count().sort("age").show(5)

print("select new columns")
df.select("id", "name", concat(df['name'], lit("---Hamada---")), "age", df["age"] + 10).show(5)
