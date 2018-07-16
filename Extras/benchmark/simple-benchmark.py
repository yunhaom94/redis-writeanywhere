#!/usr/bin/python3

import random
import string
import time
import subprocess
import os
import redis

if __name__ == "__main__": 

    size = 500 # TODO: make is an command line argument
    port = 5000
    FNULL = open(os.devnull, 'w')

    test_set = {}
    print("generating test sets")
    for i in range(size):
        key = ''.join(random.choice(string.ascii_lowercase) for _ in range(5))
        val = ''.join(random.choice(string.ascii_lowercase) for _ in range(5))

        test_set[key] = val

    print("running tests...")

    r = redis.StrictRedis(host='localhost', port=port, db=0)

    start = time.time()

    print("testing set")
    for k,v in test_set.items():
        r.set(k, v)

    print("testing get")
    for k,v in test_set.items():
        r.get(k)

    end = time.time()
    runtime = end - start
    ops = size * 2
    throughput = float(ops/runtime)
    latency = float(1/throughput)
    print("total run time: {runtime}s \n\
number of total operations with 50% Set and 50% Get: {ops} \n\
avg. throughput: {throughput} ops/s \n\
avg. latency: {latency} s".format(
               runtime=runtime,
               ops=ops,
               throughput=throughput,
               latency=latency
           ))

