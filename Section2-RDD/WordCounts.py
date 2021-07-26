from typing import List
from pyspark import SparkContext
import re


def parseLine(line: str) -> List:
    #return re.split(pattern=r"\W+", string=line.lower(), flags=re.UNICODE)
    return re.compile(pattern=r"\W+", flags=re.UNICODE).split(string=line.lower())


sc = SparkContext(master="local[*]", appName="Word Count").getOrCreate()
lines = sc.textFile(name="data/book.txt")
words = lines.flatMap(parseLine)
result = words.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y).map(lambda t: (t[1], t[0])).sortByKey().collect()


for count, word in result:
    # w = word.encode(encoding='ascii', errors='ignore')
    print(f"{word} {count}")
