#!/usr/bin/env python3
"""Run and time an optimal TSP solution."""

import sys
import time
import itertools

RUNS = 1

import math

def cost(route):
    sum = 0
    # Go back to the start when done.
    route.append(route[0])
    while len(route) > 1:
        p0, *route = route
        sum += math.sqrt((int(p0[0]) - int(route[0][0]))**2 + (int(p0[1]) - int(route[0][1]))**2)
    return sum

ts=time.time()
d = float("inf")
data=[['399', '123'], ['431', '22'], ['236', '104'], ['259', '235'], ['8', '314'], ['18', '349'], ['144', '449'], ['459', '381'], ['385', '275']]
for p in itertools.permutations(data):
    c = cost(list(p))
    if c <= d:
        d = c
        pmin = p
print("Optimal route:", pmin)
ts1=time.time()
print("Length:", d)
print("Time taken",ts1-ts)

