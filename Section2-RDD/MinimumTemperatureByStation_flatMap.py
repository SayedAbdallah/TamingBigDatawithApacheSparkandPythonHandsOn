"""
Try to find the minimum Temperature by Station id using flatMap function
the file schema is StationId, Date, ObservationType, Temperature ...
first flatMap the file to get only TMIN rows with format (StationId, Temperature)
then Reduce by key,StationId, to find the min temperature by Station Id
"""

from pyspark import SparkConf, SparkContext
from typing import Tuple, List


def parseLine(line: str) -> Tuple:
    fields = line.split(',')
    stationId = fields[0]
    observationType = fields[2]
    temperature = float(fields[3]) * 0.1  # because it stored as int while 75 is actually 7.5

    if observationType == 'TMIN':
        return [(stationId, temperature)]
    else:
        return []


conf = SparkConf().setMaster("local[*]").setAppName("Temperature By Station")
sc = SparkContext(conf=conf)


# read file as RDD of string
lines = sc.textFile(name="data/1800.csv")

# map lines to get used fields only
parsedLines = lines.flatMap(parseLine)

# reduce by key, stationId, to find the min temperature for each station
minTempByStation = parsedLines.reduceByKey(lambda x, y: min(x, y))

for temp in minTempByStation.collect():
    print(f"minimum Temperature for Station {temp[0]} is {temp[1]}")



