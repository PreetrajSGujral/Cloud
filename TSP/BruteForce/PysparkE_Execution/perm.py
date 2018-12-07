#!/usr/bin/env python3
"""Run and time an optimal TSP solution."""

import sys
import time
import itertools
import math
from pyspark import SparkContext


sc=SparkContext.getOrCreate()
RUNS = 1


def cost(route):
    sum = 0
    # Go back to the start when done.
    route.append(route[0])
    while len(route) > 1:
        p0, *route = route
        sum += math.sqrt((int(p0[0]) - int(route[0][0]))**2 + (int(p0[1]) - int(route[0][1]))**2)
    return sum

d = float("inf")
data=[['399', '123'], ['431', '22'], ['236', '104'], ['259', '235'], ['8', '314'], ['18', '349'], ['144', '449'], ['459', '381'], ['385', '275']]
p =list(itertools.permutations(data))
routes=sc.parallelize(p)

ts=time.time()
r1=routes.map(lambda x: (x, cost(list(x)))).sortByKey(True).take(3)    
print("Optimal route:",r1)
ts1=time.time()
print("Time taken: ",ts1-ts)

