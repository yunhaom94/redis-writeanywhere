#!/usr/bin/python3

import random
import string
import time
import subprocess
import os
import redis
import threading



def generate_string(string_size, size, dict):
    '''
    https://stackoverflow.com/questions/16308989/fastest-method-to-generate-big-random-string-with-lower-latin-letters
    '''


    for i in range(size):
        min_lc = ord(b'a')
        len_lc = 26
        key = bytearray(random.getrandbits(8*string_size).to_bytes(string_size, 'big'))
        for i, b in enumerate(key):
            key[i] = min_lc + b % len_lc # convert 0..255 to 97..122


        key = key.decode()
        val = key
 
        dict[key] = val




if __name__ == "__main__": 

    size = 1000 # TODO: make is an command line argument
    port = 7000
    FNULL = open(os.devnull, 'w')
    string_size = 100000
    partition = int(size/4)
    
    print("generating test sets")

    d1 = {}
    d2 = {}
    d3 = {}
    d4 = {}

    t1 = threading.Thread(target=generate_string, args = (string_size, partition, d1))
    t2 = threading.Thread(target=generate_string, args = (string_size, partition, d2))
    t3 = threading.Thread(target=generate_string, args = (string_size, partition, d3))
    t4 = threading.Thread(target=generate_string, args = (string_size, partition, d4))

    t1.start()
    t2.start()
    t3.start()
    t4.start()

    t1.join()
    t1.join()
    t1.join()
    t1.join()

    test_set = {}
    test_set.update(d1)
    test_set.update(d2)
    test_set.update(d3)
    test_set.update(d4)

    print(len(test_set))
    print("running tests...")

    r = redis.StrictRedis(host='localhost', port=port, db=0)

    start = time.time()

    print("testing set")
    for k,v in test_set.items():
        r.set(k, v)
        r.wait(3, 0)

    print("testing get")
    for k,v in test_set.items():
        r.get(k)
        r.wait(3, 0)

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

