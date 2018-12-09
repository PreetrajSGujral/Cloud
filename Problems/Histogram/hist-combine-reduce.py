#!/usr/bin/env python2.7
import sys
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL)

count = 0
currentKey = None

for line in sys.stdin:
    line = line.strip()
    key, pcount = line.split('\t') # read stdin and get (key, pcount)
	

    if currentKey == key:
        count = count + int(pcount)
    else:
        if currentKey is not None:
            print '%s\t%d' % (currentKey, count)
        count = int(pcount)
        currentKey = key

if currentKey is not None:
    print '%s\t%d' % (currentKey, count)

