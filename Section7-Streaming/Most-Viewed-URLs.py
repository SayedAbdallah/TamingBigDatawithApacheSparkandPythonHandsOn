from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, current_timestamp, window


def getColumns():
    # Parse out the common log format to a DataFrame
    hostExp = r'(^\S+\.[\S+\.]+\S+)\s'
    timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
    statusExp = r'\s(\d{3})\s'
    contentSizeExp = r'\s(\d+)$'
    generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'

    hostCol = regexp_extract(str=col("value"), pattern=hostExp, idx=1).alias("host")
    timeCol = regexp_extract(str=col("value"), pattern=timeExp, idx=1).alias("timeStamp")
    statusCol = regexp_extract(str=col("value"), pattern=statusExp, idx=1).cast("int").alias('status')
    contentSizeCol = regexp_extract(str=col("value"), pattern=contentSizeExp, idx=1).cast("int").alias('contentSize')
    methodCol = regexp_extract(str=col("value"), pattern=generalExp, idx=1).alias("method")
    endpointCol = regexp_extract(str=col("value"), pattern=generalExp, idx=2).alias("endpoint")
    protocolCol = regexp_extract(str=col("value"), pattern=generalExp, idx=3).alias('protocol')
    eventTimeCol = current_timestamp().alias('eventTime')

    return hostCol, timeCol, statusCol, contentSizeCol, methodCol, endpointCol, protocolCol, eventTimeCol


def startDemo():
    spark = SparkSession.builder.appName('Log Analysis').master('local[*]').getOrCreate()

    # readStaticDataFrame(spark=spark)

    accessLines = spark.readStream.text(path="../data/logs/")
    df = accessLines.select(*getColumns())

    # window function
    groupedDF = df.groupby(
        window(timeColumn=col('eventTime'), windowDuration='30 seconds', slideDuration='5 seconds'),
        col('endpoint')
    ).count()

    sortedDF = groupedDF.select('window.start', "window.end", "endpoint", "count").sort(col('count').desc())

    output = sortedDF.writeStream.outputMode(outputMode='complete').queryName("logs").format(source="console").start()
    output.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    startDemo()
