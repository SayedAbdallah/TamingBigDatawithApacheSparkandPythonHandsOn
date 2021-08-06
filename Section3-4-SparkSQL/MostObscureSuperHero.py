from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, size, sum, min
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def startDemo():

    # create SparkSession Object
    spark = SparkSession.builder.appName("Super Hero").getOrCreate()

    # read file as one column called value that contains the entire line
    lines = spark.read.text(paths="../data/Marvel-graph.txt")

    # split each line and get first element as the hero id
    # get the count of connections by split the line on space then count how many elements after split subtract
    # one because the hero id and name this column as connections
    # then group by id column and sum connections to get total number of connections for each hero because a hero
    # can span multiple lines

    connections = lines \
        .withColumn("id", split(col("value"), " ")[0]) \
        .withColumn("connections", size(split(col("value"), " ")) - 1) \
        .groupby(col("id")) \
        .agg(sum(col("connections")).alias("connections"))

    minNumberOfConnections = connections.agg(min(col('connections'))).first()[0]

    # filter based on connections is only one
    mostObscures = connections.filter(col("connections") == minNumberOfConnections)

    # read hero names and get the hero name
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])

    names = spark.read.schema(schema).option("sep", " ").csv(path="../data/Marvel-names.txt")

    mostObscures.join(names, on="id").show()

    spark.stop()


if __name__ == "__main__":
    startDemo()
