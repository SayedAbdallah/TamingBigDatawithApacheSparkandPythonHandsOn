"""
Try to find the minimum Temperature by Station id
the file schema is StationId, Date, ObservationType, Temperature ...
first map the file to (StationId, ObservationType, Temperature)
second filter TMIN observation types
third map to (StationId, Temperature)
fourth Reduce by key,StationId, to find the min temperature by Station Id
"""

from pyspark import SparkConf, SparkContext
from typing import Tuple


def parseLine(line: str):
    fields = line.split(',')
    stationId = fields[0]
    observationType = fields[2]
    temperature = float(fields[3]) * 0.1  # because it stored as int while 75 is actually 7.5

    return stationId, observationType, temperature


conf = SparkConf().setMaster("local[*]").setAppName("Temperature By Station")
sc = SparkContext(conf=conf)


# read file as RDD of string
lines = sc.textFile(name="data/1800.csv")

# map lines to get used fields only
parsedLines = lines.map(parseLine)

# filter on ObservationType equals TMIN only
minTemperatures = parsedLines.filter(lambda x: x[1] == 'TMIN')

# we don't need observation type any more so map to remove it
stationTemperatures = minTemperatures.map(lambda x: (x[0], x[2]))

# reduce by key, stationId, to find the min temperature for each station
minTempByStation = stationTemperatures.reduceByKey(lambda x, y: min(x, y))

for temp in minTempByStation.collect():
    print(f"minimum Temperature for Station {temp[0]} is {temp[1]}")



