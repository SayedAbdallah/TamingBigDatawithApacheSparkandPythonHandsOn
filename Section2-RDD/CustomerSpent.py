"""
Try to find out total spent amount by customer
file schema is CustomerId, ProductId, amount
"""
from pyspark import SparkContext
from typing import Tuple


def parseLine(line: str) -> Tuple:
    fields = line.split(',')
    customer = fields[0]
    amount = float(fields[2])
    return customer, amount


sc = SparkContext(master="local[*]", appName="Total spent by Customer")
lines = sc.textFile(name="data/customer-orders.csv")
customerAmount = lines.map(parseLine)  # RDDs of  (CustomerId, amount)
totalAmountByCustomer = customerAmount.reduceByKey(lambda amount1, amount2: amount1 + amount2).sortBy(lambda i: i[1])

for customer, amount in totalAmountByCustomer.collect():
    print(f"Customer {customer} total spent amount is {amount:.2f} $")
