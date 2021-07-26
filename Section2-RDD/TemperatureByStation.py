"""
Try to find the minimum and maximum Temperature by Station id
the file schema is StationId, Date, ObservationType, Temperature ...
first map the file to (StationId, ObservationType, Temperature)
second filter TMIN, TMAX observation types
third map to (StationId, Temperature)
fourth Reduce by key,StationId, to find the min and max temperature by Station Id
"""

from pyspark import SparkConf, SparkContext
from typing import Tuple


def parseLine(line: str) -> Tuple:
    fields = line.split(',')
    stationId = fields[0]
    observationType = fields[2]
    temperature = float(fields[3]) * 0.1  # because it stored as int while 75 is actually 7.5

    return stationId, observationType, temperature


conf = SparkConf().setMaster("local[*]").setAppName("Temperature By Station")
sc = SparkContext(conf=conf)


# read file as RDD of string
lines = sc.textFile(name="data/1800.csv")

# map lines to get stationId ObservationType and Temperature
parsedLines = lines.map(parseLine)

# filter by observationType to get two RDDs one for min temperatures and one for max temperature
minTemperature = parsedLines.filter(lambda r: r[1] == 'TMIN')
maxTemperature = parsedLines.filter(lambda r: r[1] == 'TMAX')

# we don't need observationType any mote so remove it and map RDD to be (StationId, Temperature)
minTemp = minTemperature.map(lambda x: (x[0], x[2]))
maxTemp = maxTemperature.map(lambda x: (x[0], x[2]))

# reduce by key, stationId, to find the min temperature for each station
minTempByStation = minTemp.reduceByKey(lambda t1, t2: min(t1, t2))
maxTempByStation = maxTemp.reduceByKey(lambda t1, t2: max(t1, t2))

# join the two RDDs
temperaturesByStation = minTempByStation.join(maxTempByStation)
for temp in temperaturesByStation.collect():
    print(f"Station {temp[0]} minimum Temperature is {temp[1][0]:2f}  and maximum Temperature is {temp[1][1]:.2f}")

