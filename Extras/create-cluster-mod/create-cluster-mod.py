#!/usr/bin/python3
import argparse
import subprocess
import os
import time


'''
Usage: python3 create-cluster-mod.py num_nodes
Will run n nodes, set them slots and assgin them to each other's slave(to set them in the same group)
'''

if __name__ == '__main__':
    FNULL = open(os.devnull, 'w')
    parser = argparse.ArgumentParser()
    parser.add_argument("num_nodes", type=int, help="number of nodes")
    num_nodes = parser.parse_args().num_nodes

    port_base = 7000

    # starting
    for i in range(0, num_nodes):
        port = port_base + i
        print("starting: " + str(port))

        cmd = "../../src/redis-server --port {port} \
--cluster-enabled yes --cluster-config-file nodes-{port}.conf \
--cluster-node-timeout 2000 --appendonly yes --appendfilename \
appendonly-{port}.aof --dbfilename dump-{port}.rdb --logfile {port}.log \
--daemonize no --protected-mode no".format(port=port)
        subprocess.Popen(cmd, shell=True, stdout=FNULL)
        #print(cmd)

    print("Please wait for the cluster to be set up...")
    time.sleep(1) # wait for the cluster to run
    # meeting
    for i in range(1, num_nodes):
        port = port_base + i

        cmd = "../../src/redis-cli -p {port_base} CLUSTER MEET 127.0.0.1 {port}".format(port_base=port_base, port=port)
        subprocess.call(cmd, shell=True, stdout=FNULL)


    # setting slots:
    # TODO: this is slow
    for i in range(0, num_nodes): 
        port = port_base + i
        for s in range(0, 16384):
            cmd = "../../src/redis-cli -p {port} CLUSTER ADDSLOTS {s}".format(port=port, s=s)
            subprocess.call(cmd, shell=True, stdout=FNULL)

    # getting cluster data
    cluster_nodes = {}
    cmd = "../../src/redis-cli -p {port_base} CLUSTER NODES".format(port_base=port_base)
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
    result = p.stdout.decode()

    for line in result.split("\n"):
        info = line.split(" ") 
        if len(info) > 2:
            id = info[0].strip()
            port = info[1].split("@")[0].split(":")[1].strip()
            cluster_nodes[port] = id

        else:
            continue

    # set replicate

    for i in range(0, num_nodes):
        port = port_base + i

        for p, n in cluster_nodes.items():
            if str(p) != str(port):
                cmd = "../../src/redis-cli -p {port} CLUSTER REPLICATE {node}".format(port=port, node=n)
                subprocess.call(cmd, shell=True, stdout=FNULL)

    try:
        print("Servers are running, use ctrl+c to stop and clean the cluster")
        while True:
            continue
    except KeyboardInterrupt:
        pass


    # stopping

    for i in range(0, num_nodes):
        port = port_base + i
        print("stopping and cleaning: " + str(port))
        
        cmd = "../../src/redis-cli -p {port} shutdown nosave".format(port=port)
        subprocess.call(cmd, shell=True, stdout=FNULL)


    # cleaning
    cmd = "rm -rf *.log appendonly*.aof dump*.rdb nodes*.conf"
    subprocess.call(cmd, shell=True, stdout=FNULL)
