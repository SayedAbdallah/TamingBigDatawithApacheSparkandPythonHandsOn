from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql.functions import sum, round, desc

schema = StructType([
    StructField(name="customerId", dataType=IntegerType(), nullable=True),
    StructField(name="ItemId", dataType=IntegerType(), nullable=True),
    StructField(name="amount", dataType=FloatType(), nullable=True)
])

spark = SparkSession.builder.appName("Total amount spent by Customer").master("local").getOrCreate()
df = spark.read.schema(schema=schema).csv("../data/customer-orders.csv")

result = df.groupby('customerId').agg(round(sum("amount"), 2).alias('TotalAmount')).sort(desc('TotalAmount'))
result.show(result.count())

spark.stop()
