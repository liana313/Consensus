# This module provides a very simple client interface for suggesting new
# replicated values to one of the servers. No reply is received so an eye must
# be kept on the server output to see if the new suggestion is received. Also,
# when master leases are in use, requests must be sent to the current master
# server. All non-master servers will ignore the requests since they do not have
# the ability to propose new values in the multi-paxos chain.

import sys

from datetime import datetime

from datetime import time
 
from twisted.internet import reactor, defer, protocol

import config

class ClientProtocol(protocol.DatagramProtocol):

    def __init__(self, uid, new_value):
    	# TODO: fix later - A is hardcoded as leader proposer - all clients will write to server A's ip, port
        self.addr      = config.server[sys.argv[1]]#config.peers['A']
        self.new_value = new_value
        self.client_id = sys.argv[1] #A, B or C

    def startProtocol(self):
    	text = bytes('propose {0} {1} {2}'.format(self.new_value, datetime.now().time(), self.client_id), 'utf-8')
    	self.transport.write(text, self.addr)
    	#reactor.stop()
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


if len(sys.argv) != 3 or not  sys.argv[1] in config.server:
    print('python client.py <id of lead proposer: 1000, 2000, 3000 or 4000> <sndr id>-,rcvr id>-<amount>')
    sys.exit(1)

    
def main():
    reactor.listenUDP(config.client[sys.argv[1]][1],ClientProtocol(sys.argv[1], sys.argv[2]))

    
reactor.callWhenRunning(main)
reactor.run()


# run on client: python client.py <A|B|C> <new_value>
# run on server: python server.py <A|B|C>
# TODO - just need the buffer

