#!/usr/bin/env python
import sys
import glob
import os
import socket
import hashlib
import traceback
import numpy
from datetime import datetime
from time import time
from time import sleep

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from cassandra import keyValueStoreService
from cassandra.ttypes import SystemException, NodeID


class keyValueStoreServer:
    def __init__(self, key_node_map = None, key_replica_map = None):
        self.curr_node = NodeID()
        self.key_val_store = dict()
        self.backup_store = dict()
        self.key_node_map = dict()
        self.key_replica_map = dict()
        self.my_range = []
        self.ip = socket.gethostbyname(socket.gethostname())
        self.port = numpy.uint32(sys.argv[1])
        self.replicationfactor = 3
        self.hintedHandoffQueue = [[]] * 4
        self.all_nodes = dict()
        self.isHintedHandoff = 0


    def storeNodeMappings(self, index, node_ip, node_port, key_range):
        try:
            if (index, node_ip, node_port) not in self.key_node_map:
                self.key_node_map.update({(index, node_ip, node_port): key_range})
            else:
                self.key_node_map.update({(index, node_ip, node_port): key_range})
        except:
            print(traceback.format_exc())
        #print("Store of other nodes ", self.key_node_map)

           

    def updateWriteAheadLog(self, key, value):
        #try:
        host_name = socket.gethostname()
        self_ip_addr = socket.gethostbyname(host_name)
        port_number = sys.argv[1]
        #print("With date", datetime.now())
        #print("Without date ", datetime.now().time())
        file_name = self_ip_addr + ':' + port_number + '.txt'
        f = open(file_name, "a")
        log = str(key) + ':' + str(value) + ':' + str(datetime.now()) + '\n'
        f.write(log)
        f.close()
        #except:
            #print(traceback.format_exc())

    def storeKeyValue(self, key, value, t=None):
        if(self.isHintedHandoff == 0):
            if key in self.key_val_store:
                self.key_val_store.update({key: (value, time() * 1000)})
            else:
                self.key_val_store.update({key: (value, time() * 1000)})
            print("dict values are ", self.key_val_store)
        elif(self.isHintedHandoff == 1):
            print("ISHINTEDHANDOFF")
            if key in self.key_val_store:
                self.key_val_store.update({key: (value, t)})
            else:
                self.key_val_store.update({key: (value, t)})
            print("dict values are ", self.key_val_store)
   

    def tryHandoff(self, nodeip, nodeport, i):
        i = i - 1
        # print("Node is ", node.ip, node.port)
        try:
            nodeip = str(nodeip)
            socket = TSocket.TSocket(nodeip, numpy.uint32(nodeport))
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = keyValueStoreService.Client(protocol)
            print("Node is ", nodeip, nodeport)
            # print(self.hintedHandoffQueue[i][0], self.hintedHandoffQueue[i][1], self.hintedHandoffQueue[i][2])
            transport.open()
            client.storeKeyValue(self.hintedHandoffQueue[i][0], self.hintedHandoffQueue[i][1],
                                 self.hintedHandoffQueue[i][2])

            self.hintedHandoffQueue[i] = []
            print(self.hintedHandoffQueue[i])
            transport.close()
            repfailed = 0
            self.isHintedHandoff = 0
            return repfailed
        except :
            print (SystemException())


    def putKeyValue(self, key, value, consistency):
        node = NodeID()
        try:
            self.updateWriteAheadLog(key, value)
            replica1_index = 0
            replica2_index = 0
            healthy_server = 0
            indices = [0]*4
            replicasFailed = [0]*4
            nodeports = [0]*4
            nodeips = [0]*4
            for(index, node.ip, node.port), key_range in self.key_node_map.items():
                if key_range[0] <= key and key <= key_range[1]:
                    print("key range is ", key_range)
                    replica1_index = (index + 1) % 5
                    replica2_index = (index + 2) % 5
                    print("Replicas are ", replica1_index, replica2_index)
                    #print("My node id is ", self.id)
                    #print("Node id is ", node.id)
                    #print("my port and ip are ", type(self.ip), type(self.port))
                    print("Node ip and node port is ", node.ip, node.port)
                    if self.ip != node.ip or self.port != node.port:
                        #print("Hi")
                        try:
                            socket = TSocket.TSocket(node.ip, node.port)
                            #print("Node is ", node.ip, node.port)
                            transport = TTransport.TBufferedTransport(socket)
                            protocol = TBinaryProtocol.TBinaryProtocol(transport)
                            client = keyValueStoreService.Client(protocol)
                            transport.open()
                            client.storeKeyValue(key, value)
                            transport.close()
                            healthy_server = healthy_server + 1
                        except:
                            print("HINTED HANDOFF HERE REP1")
                            nodeports[0] = node.port
                            nodeips[0] = node.ip
                            indices[0] = index
                            self.hintedHandoffQueue[index-1] = [key, value, time()*1000]
                            replicasFailed[0] = -100
                            # exception will be generated if the above server is not working
                            pass
                    else:
                        healthy_server = healthy_server + 1
                        self.storeKeyValue(key, value)

                    #contact all replicas
                    for(index, node.ip, node.port) in self.key_node_map:
                        if index == replica1_index:
                            # print("Replica nodes are ", node.id)
                            if self.ip != node.ip or self.port != node.port:
                                try:
                                    #print("My ip and port are ", self.ip, self.port)
                                    socket = TSocket.TSocket(node.ip, node.port)
                                    #print("Node is ", node.ip, node.port)
                                    transport = TTransport.TBufferedTransport(socket)
                                    protocol = TBinaryProtocol.TBinaryProtocol(transport)
                                    client = keyValueStoreService.Client(protocol)
                                    transport.open()
                                    client.storeKeyValue(key, value)
                                    transport.close()
                                    healthy_server = healthy_server + 1
                                except:
                                    print("HINTED HANDOFF HERE REP2")
                                    nodeports[1] = node.port
                                    nodeips[1] = node.ip
                                    indices[1] = index
                                    self.hintedHandoffQueue[index-1] = [key, value, time() * 1000]
                                    replicasFailed[1] = -100
                                    #the above server is not working
                                    pass
                            else:
                                self.storeKeyValue(key, value)
                                healthy_server = healthy_server + 1
                        elif index == replica2_index:
                            #print("Replica nodes are ", node.id)

                            if self.ip != node.ip or self.port != node.port:
                                try:
                                    #print("My ip and port are ", self.ip, self.port)
                                    socket = TSocket.TSocket(node.ip, node.port)
                                    #print("Node is ", node.ip, node.port)
                                    transport = TTransport.TBufferedTransport(socket)
                                    protocol = TBinaryProtocol.TBinaryProtocol(transport)
                                    client = keyValueStoreService.Client(protocol)
                                    transport.open()
                                    client.storeKeyValue(key, value)
                                    transport.close()
                                    healthy_server = healthy_server + 1
                                except:
                                    print("HINTED HANDOFF HERE REP3")
                                    nodeports[2] = node.port
                                    nodeips[2] = node.ip
                                    indices[2] = index
                                    self.hintedHandoffQueue[index-1] = [key, value, time() * 1000]
                                    replicasFailed[2] = -100
                                    #the above server is not working
                                    pass
                            else:
                                self.storeKeyValue(key, value)
                                healthy_server = healthy_server + 1
                    replica1_index = 0
                    replica2_index = 0
            print (self.hintedHandoffQueue)
            if healthy_server >= 2:
                print("HINTED HANDOFF WORKING")
                if replicasFailed[0] == -100:
                    print("8 seconds hintedhandoff window ")
                    sleep(8)
                    self.isHintedHandoff = 1
                    try:
                        replicasFailed[0] = self.tryHandoff(
                            nodeips[0], nodeports[0], indices[0])
                    except:
                        self.isHintedHandoff = 0
                if replicasFailed[1] == -100:
                    print("8 seconds hintedhandoff window ")
                    sleep(60)
                    self.isHintedHandoff = 1
                    try:
                        print("INTO FUNCTION 1")
                        replicasFailed[1] = self.tryHandoff(
                            nodeips[1], nodeports[1], indices[1])
                    except:
                        self.isHintedHandoff = 0
                if replicasFailed[2] == -100:
                    print("8 seconds hintedhandoff window ")
                    sleep(8)
                    self.isHintedHandoff = 1
                    try:
                        replicasFailed[2] = self.tryHandoff(
                            nodeips[2], nodeports[2], indices[2])
                    except:
                        self.isHintedHandoff = 0
            print (self.hintedHandoffQueue)
            if consistency == "one" or consistency == "One":
                if healthy_server >= 1:
                    string = "Write is successful"
                    return True
                else:
                    string = "Write was not successful"
                    del self.hintedHandoffQueue
                    self.hintedHandoffQueue = [[]] * 4
                    return False

            elif consistency == "quorum" or consistency =="Quorum":
                if healthy_server >= 2:
                    string = "Write is successful"
                    return True
                else:
                    string = "Write was not successful"
                    del self.hintedHandoffQueue
                    self.hintedHandoffQueue = [[]] * 4
                    return False
                    #print("Number of down servers", faulty_server)      
        except:
            print(traceback.format_exc())



    def readKey(self, key):
        try:
            return self.key_val_store.get(key)
        except:
            print(traceback.format_exc()) 

    def getKey(self, key, consistency): 
        node = NodeID()
        try:
            replica1_index = 0
            replica2_index = 0
            healthy_server = 0
            values = []
            #if consistency == "one" or consistency == "One":
            for(index, node.ip, node.port), key_range in self.key_node_map.items():
                if key_range[0] <= key and key <= key_range[1]:
                    print("key range is ", key_range)
                    replica1_index = (index + 1) % 5
                    replica2_index = (index + 2) % 5
                    print("Replicas are ", replica1_index, replica2_index)
                    #print("My node id is ", self.id)
                    #print("Node id is ", node.id)
                    #print("my port and ip are ", type(self.ip), type(self.port))
                    print("Node ip and node port is ", node.ip, node.port)
                    if self.ip != node.ip or self.port != node.port:
                        #print("Hi")
                        try:
                            socket = TSocket.TSocket(node.ip, node.port)
                            #print("Node is ", node.ip, node.port)
                            transport = TTransport.TBufferedTransport(socket)
                            protocol = TBinaryProtocol.TBinaryProtocol(transport)
                            client = keyValueStoreService.Client(protocol)
                            transport.open()
                            values.append(client.readKey(numpy.uint32(key)))
                            transport.close()
                            healthy_server = healthy_server + 1
                        except:
                            #the above server is not working
                            continue
                    else:
                        healthy_server = healthy_server + 1
                        values.append(self.readKey(numpy.uint32(key)))


                    #contact all replicas
                    for(index, node.ip, node.port) in self.key_node_map:
                        if index == replica1_index or index == replica2_index:
                            #print("Replica nodes are ", node.id)
                            if self.ip != node.ip or self.port != node.port:
                                try:
                                    #print("My ip and port are ", self.ip, self.port)
                                    socket = TSocket.TSocket(node.ip, node.port)
                                    #print("Node is ", node.ip, node.port)
                                    transport = TTransport.TBufferedTransport(socket)
                                    protocol = TBinaryProtocol.TBinaryProtocol(transport)
                                    client = keyValueStoreService.Client(protocol)
                                    transport.open()
                                    values.append(client.readKey(numpy.uint32(key)))
                                    transport.close()
                                    healthy_server = healthy_server + 1
                                except:
                                    #the above server is not working
                                    continue
                            else:
                                values.append(self.readKey(numpy.uint32(key)))
                                healthy_server = healthy_server + 1
                    replica1_index = 0
                    replica2_index = 0

                    for val in values:
                        print("The values are ", val)
                    """if consistency == "one" or consistency =="One":
                        if healthy_server >= 1:
                            return "Write is successful"
                        else:
                            return "Wite was not successful"

                    elif consistency == "quorum" or consistency =="Quorum":
                        if healthy_server >= 2:
                            return "Write is successful"
                        else:
                            return "Write was not successful"""
            
            return "HEY"      
        except:
            print(traceback.format_exc())

if __name__ == '__main__':
    server_obj = keyValueStoreServer()
    #server_obj.readAllNodes()
    transport = TSocket.TServerSocket(port=int(sys.argv[1]))
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    processor = keyValueStoreService.Processor(server_obj)

    cassandra_server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    print("Server is running on", sys.argv[1])
    cassandra_server.serve()

