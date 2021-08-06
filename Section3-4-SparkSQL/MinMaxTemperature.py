from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import min, max, col, round

spark = SparkSession.builder.appName("Min and Max Temperature By Station").master("local").getOrCreate()

schema = StructType([
    StructField(name="stationId", dataType=StringType(), nullable=True),
    StructField(name="date", dataType=IntegerType(), nullable=True),
    StructField(name="observationType", dataType=StringType(), nullable=True),
    StructField(name="temperature", dataType=FloatType(), nullable=True)
])


df = spark.read.schema(schema=schema).csv("../data/1800.csv")
minTemperatures = df.where(df.observationType == 'TMIN').select(df.stationId, (col("temperature") * 0.1).alias("temperature"))
maxTemperatures = df.where(df.observationType == 'TMAX').select(df.stationId, (col("temperature") * 0.1).alias("temperature"))

minTemp = minTemperatures.groupby("stationId").min("temperature").withColumnRenamed('min(temperature)', 'minTemperature')
maxTemp = maxTemperatures.groupby("stationId").agg(round(max("temperature"), 2)).withColumnRenamed('round(max(temperature), 2)', 'maxTemperature')

result = minTemp.join(maxTemp, on="stationID")
result.show()

spark.stop()
