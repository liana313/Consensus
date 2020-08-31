# This module encapsulates the networking strategy for the application. JSON
# encoded UDP packets are used for all communication.
#

import json

from twisted.internet import reactor, protocol

#from composable_paxos import ProposalID

import config #TODO remove this import and make client port an arg that is stored locally as var


class Messenger(protocol.DatagramProtocol):

    def __init__(self, uid, peer_addresses, replicated_val):
        self.addrs          = dict(peer_addresses)
        self.replicated_val = replicated_val

        # provide two-way mapping between endpoints and server names
        for k,v in list(self.addrs.items()):
            self.addrs[v] = k

        reactor.listenUDP( peer_addresses[uid][1], self )

        
    def startProtocol(self):
        self.replicated_val.set_messenger(self)

        
    def datagramReceived(self, packet0, from_addr):
        packet = str(packet0, 'utf-8')
        #print(packet)
        #print('received\n')
        try:
            
            message_type, data = packet.split(' ', 1)
            #print("message_type: ", message_type, "data: ", data)

            if message_type == 'propose':
                #print("propose message")

                new_val, t, c = data.split(' ', 2)
                print("t: ", t, " new_val: ", new_val, " c: ", c)
                self.replicated_val.propose_update( new_val, t, c )

            else:
                from_uid = self.addrs[from_addr]

                print('rcv', from_uid, ':', packet)

                # Dynamically search the class for a method to handle this message
                handler = getattr(self.replicated_val, 'receive_' + message_type, None)

                if handler:
                    kwargs = json.loads(data)

                    # for k in kwargs.keys():
                    #     if k.endswith('_id') and kwargs[k] is not None:
                    #         # JSON encodes the proposal ids as lists,
                    #         # composable-paxos requires requires ProposalID instances
                    #         kwargs[k] = ProposalID(*kwargs[k])
                        
                    handler(from_uid, **kwargs)
            
        except Exception:
            print('Error processing packet: ', packet)
            import traceback
            traceback.print_exc()
            

    def _send(self, to_uid, message_type, **kwargs):
        msg = '{0} {1}'.format(message_type, json.dumps(kwargs))
        print('snd', to_uid, ':', msg)
        text = bytes(msg, 'utf-8')
        self.transport.write(text, self.addrs[to_uid])

    def send_preprepare(self, peer_uid, v, n, m, i):
        #print("---------messenger calling send to :", peer_uid)
        self._send(peer_uid, 'preprepare', view = v, seq_num = n, message = m, id = i)


    def send_prepare(self, peer_uid, v, n, d, i):
        self._send(peer_uid, 'prepare', view = v, seq_num = n, digest = d, id = i)

    def send_commit(self, peer_uid, v, n, d, i):
        self._send(peer_uid, 'commit', view = v, seq_num = n, digest = d, id = i)

    def send_reply(self, new_val):
        msg = '{0} {1} {2}'.format('reply', new_val, self.replicated_val.get_network_uid())
        print('snd client:', msg)
        text = bytes(msg, 'utf-8')
        self.transport.write(text, config.client)

    # TODO: add send_reply - when accepted is received by learner, send value to client, client will continually check for messages and ignore those with same seq/proposal num

