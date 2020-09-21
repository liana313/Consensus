# This module provides a very simple client interface for suggesting new
# replicated values to one of the servers. No reply is received so an eye must
# be kept on the server output to see if the new suggestion is received. Also,
# when master leases are in use, requests must be sent to the current master
# server. All non-master servers will ignore the requests since they do not have
# the ability to propose new values in the multi-paxos chain.

import sys
import time
import random

from twisted.internet import reactor, defer, protocol

import config

class ClientProtocol(protocol.DatagramProtocol):

    def __init__(self, uid, req_count, p_cross_cluster, mobility_count):
    	# TODO: fix later - A is hardcoded as leader proposer - all clients will write to server A's ip, port
        self.addr      = config.server[sys.argv[1]]
        self.uid = uid
        self.req_count = int(req_count)
        self.p_cross_cluster = float(p_cross_cluster)
        self.mobility_count = int(mobility_count) 

    def startProtocol(self):
        message_interval = 0.2 # 200 ms
        local_count = 0
        cross_count = 0

        for i in range(self.req_count):
            sender_id, receiver_id = self.getRandomPeers()
            message = 'propose {0}-{1}-{2}'.format(sender_id, receiver_id, random.randint(1,3))
            text = bytes(message, 'utf-8')
            self.transport.write(text, self.addr)
            time.sleep(message_interval)

            # debug info
            print(message)
            if int(sender_id / 1000) == int(receiver_id / 1000):
                local_count += 1
            else:
                cross_count += 1

        print('local message: {0}, cross-cluster messages: {1}'.format(local_count, cross_count))
    	#reactor.stop()

        for i in range(self.mobility_count):
            sender_id, receiver_id = self.getRandomPeers()
            new_cluster = random.randint(1, 4)
            message = 'mobility_req {0}-{1}'.format(sender_id, new_cluster)
            text = bytes(message, 'utf-8')
            self.transport.write(text, self.addr)
            time.sleep(message_interval)


    def getRandomPeers(self):
        cluster_count = len(config.peers[0])
        per_cluster = len(config.peers[0][1]) - 1 ## need -1 because dictionary is initiatied with bogus values

        peer_1 = int(self.uid) + random.randint(1, per_cluster)

        roll = random.random()
        if roll < self.p_cross_cluster:
            cluster_id = random.randrange(1000, cluster_count * 1000, 1000)
            # wrap around (also, avoiding zero)
            cluster_id = ((int(self.uid) + cluster_id - 1000) % (cluster_count * 1000)) + 1000

            peer_2 = cluster_id + random.randint(1, per_cluster)
        else:
            peer_2 = int(self.uid) + random.randint(1, per_cluster)
        return (peer_1, peer_2)

    def datagramReceived(self, packet0, from_addr):
        packet = str(packet0, 'utf-8')
        try:
            message_type, data, server_uid = packet.split(' ', 2)

            if message_type == 'reply':
                print("consensus!!  value: ", data, ", server: ", server_uid)
            else:
                print("unkown message recieved")
                sys.exit(1)
        except Exception:
            print('Error processing packet: ', packet)
            import traceback
            traceback.print_exc()


if len(sys.argv) != 5 or not sys.argv[1] in config.server:
    print('python client.py <id of lead proposer: 1000, 2000, 3000 or 4000> <sndr id>-,rcvr id>-<amount>')
    sys.exit(1)

    
def main():
    reactor.listenUDP(config.client[sys.argv[1]][1],ClientProtocol(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]))

    
reactor.callWhenRunning(main)
reactor.run()


# run on client: python client.py <A|B|C> <new_value>
# run on server: python server.py <A|B|C>
# TODO - just need the buffer

