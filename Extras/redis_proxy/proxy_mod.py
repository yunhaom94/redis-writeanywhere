#!/usr/bin/python3

import redis
import sys, getopt
import socket, select
import crc16

class Node():
    def __init__(self, id, address):
        self.id = id
        self.address = address
        self.flag =  None
        self.slots = None
        self.master = None

    @staticmethod
    def parse_node(raw_response):
        '''Parse response by command "CLUSTER NODES"'''
        response = raw_response.decode("utf-8").split("\n")
        response = [line for line in response if "connected"in line ]
        nodes_list = [line.split() for line in response]
        result_dict = {}

        for n in nodes_list:
            node = Node(n[0], n[1])
            if n[2] == "slave" or n[2] == "myself,slave" :
                node.flag = "slave"
                node.master = n[3]
            elif n[2] == "master" or n[2] == "myself,master":
                node.flag = "master"
                node.slots = n[8]
            else:
                raise Exception("Something wrong with CLUSTER NODE results")
            
            result_dict[node.id] = node


        result = []
        # set slots for slaves
        for k, n in result_dict.items():
            if n.flag == 'slave':
                n.slots = result_dict[n.master].slots
            result.append(n)

        return result
        

class CommandHandler():
    def __init__(self, rhost, rport):
        self.slot_node_map = {}
        self.connect_to_redis(rhost, rport)
        

    def connect_to_redis(self, host, port):
        self.node_connections = []
        first = redis.Connection(host, port)
        first.connect()

        self.node_connections.append(first)

        first._sock.sendall(b'*2\r\n$7\r\ncluster\r\n$5\r\nnodes\r\n')
        response = self.get_response(first._sock)
        self.nodes = Node.parse_node(response)

        for n in self.nodes:
            ip, port = n.address.split(":")[0], n.address.split(":")[1].split("@")[0]
            self.connect_to_node(ip, port)

    
    def connect_to_node(self, host, port):
        redis_connection = redis.Connection(host, port)
        redis_connection.connect()
        self.node_connections.append(redis_connection)


    def run(self):
        # Create a TCP/IP socket
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        print('Proxy listening to port 5000')
        self.client_socket.bind(('localhost', 5000)) # 7000 as listening port

        # Listen for incoming connections
        self.client_socket.listen(1) # only accept 1 client

        while True:
            # Wait for a connection
            print('waiting for a connection')
            connection, client_address = self.client_socket.accept()
            try:
                print('connection from', client_address)

                # Receive the data in small chunks and retransmit it
                while True:
                    data = connection.recv(1024)
                    if data:
                        self.proxy_query(data, connection)
                        
                        #print('sending data back to the client')
                        #connection.sendall(data)
                    else:
                        print('Disconnected by', client_address)
                        break

            finally:
                # Clean up the connection
                connection.close()



    def proxy_query(self, query, client):

        print("Forwarding: " +  str(query))
        
        for n in self.node_connections:
            n._sock.sendall(query)

        bflag = False
        while True:
            
            if bflag:
                break

            for n in self.node_connections:
                server_reponse = self.get_response(n._sock, 0.1)

                if not bflag and server_reponse != b'':
                    print("One node replied OK")
                    print("Response: " + str(server_reponse))
                    bflag = True # finish the for loop to read the rest of the recv buffer
                    client.sendall(server_reponse)

            

        
        
                
        
    def get_response(self, sock, timeout=0.1):
        server_reponse = b''

        while True:
            try:
                sock.settimeout(0.1)
                data = sock.recv(1024)
            
                if data:
                    server_reponse += data

                else:
                    print("Disconnected by redis")
                    return

            except BlockingIOError:
                return server_reponse

            except socket.timeout:
                sock.setblocking(True) # set back to blocking
                return server_reponse


    
def main(argv):
    host = ''
    port = ''
    try:
        opts, args = getopt.getopt(argv, "h:p:")
    except getopt.GetoptError:
        print("proxy.py -h <host> -p <port>")
        return

    for opt, arg in opts:
        if len(sys.argv) != 5:
            print("proxy.py -h <host> -p <port>")
            return
        elif opt in ("-h"):
            host = arg
        elif opt in ("-p"):
            port = arg

    ch = CommandHandler(host, port)
    ch.run()


if __name__ == "__main__": 
    main(sys.argv[1:])
