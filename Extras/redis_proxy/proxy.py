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
        self.connect_to_redis(rhost, rport, True)
        

    def connect_to_redis(self, host, port, update_map=False):
        self.redis_connection = redis.Connection(host, port)
        self.redis_connection.connect()
        self.server_socket = self.redis_connection._sock
        self.address = self.redis_connection.host + ":" + port

        print("Connected to Redis node: " + self.address)

        if update_map:
            self.update_slot_node_mapping()
    

    def update_slot_node_mapping(self):
        try:
            self.server_socket.sendall(b'*2\r\n$7\r\ncluster\r\n$5\r\nnodes\r\n')
            response = self.get_response()
            nodes = Node.parse_node(response)
            for n in nodes:

                slots = n.slots.split("-")
                begin, end = slots[0], slots[1]
                for i in range(int(begin), int(end) + 1):
                    if i not in self.slot_node_map:
                        self.slot_node_map[i] = [n]
                    else:
                        self.slot_node_map[i].append(n)

        except:
            self.slot_node_map = None



    def run(self):
        # Create a TCP/IP socket
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        print('Proxy listening to port 7000')
        self.client_socket.bind(('localhost', 7000)) # 7000 as listening port

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
        self.check_target_server(query)


        print("Forwarding: " +  str(query))
        
        self.server_socket.sendall(query)

        server_reponse = self.get_response()

        flag = server_reponse[0]
        if chr(flag) == '-':
            msg = server_reponse.decode("utf-8").split()

            if msg[0] == "-MOVED":
                rip = msg[2].split(':')[0]
                rport = msg[2].split(':')[1]
                
                print("Redirection recieved")

                self.redis_connection.disconnect()

                self.connect_to_redis(rip, rport)
                self.proxy_query(query, client)
                self.update_slot_node_mapping()
                return
                

        
        print("Response: " + str(server_reponse))
        #server_reponse = b'+OK\r\n'
        client.sendall(server_reponse)

    def check_target_server(self, query):
        '''Premerutally connect to a different redis node 
            if the current node isn't reponsible for that key
        '''
        if not self.slot_node_map:
            return

        temp = query.decode("utf-8").split()

        # has to be at east "*2...."
        if int(temp[0][1]) >= 2:
            command = temp[2]
            if command == "set" or \
               command == "get" or \
               command == "del":
            
                key = temp[4]

                tar_slot = crc16.crc16xmodem(key.encode()) % 16384
                
                nodes = self.slot_node_map[tar_slot]
                
                # TODO: change these when master nolonger needed
                address = list(filter(lambda n: n.flag == "master", nodes))[0].address.split("@")[0]
                ip, port = address.split(":")[0], address.split(":")[1]
                cur_ip, cur_port = self.address.split(":")[0], self.address.split(":")[1]

                if ip == "127.0.0.1" or ip == "localhost":
                    if cur_ip == "127.0.0.1" or cur_ip == "localhost":
                        if port == cur_port:
                            return
                       
                if address == self.address:
                    return
                       

                print("Current node not reponsible for this key! Redirecting...")
                self.redis_connection.disconnect()
                self.connect_to_redis(ip, port) 

                
                
        
    def get_response(self):
        server_reponse = b''

        while True:
            try:
                data = self.server_socket.recv(1024)
                # Set to non blocking after recieve first response
                # because response take sometime.
                # TODO: Timeout to 0.1s is probably bad though
                self.server_socket.settimeout(0.1)
                if data:
                    server_reponse += data

                else:
                    print("Disconnected by redis")
                    return
            except socket.timeout:
                self.server_socket.setblocking(True) # set back to blocking
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
