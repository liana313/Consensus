# This module encapsulates the networking strategy for the application. JSON
# encoded UDP packets are used for all communication.
#

import json

from twisted.internet import reactor, protocol
from datetime import datetime

from datetime import time

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

            if message_type == 'propose_update':
                new_val, t, c = data.split(' ', 2)
                self.replicated_val.propose_update( new_val, t, c)
            elif message_type == 'mobility_req':
                node, new_cluster = data.split('-', 1)
                self.replicated_val.receive_mobility_req(node, int(new_cluster))
            elif message_type == 'account':
                print("rcv: ", packet)
                handler = getattr(self.replicated_val, 'receive_' + message_type, None)
                kwargs = json.loads(data)
                handler(**kwargs)
            elif message_type == 'propose':
                #print("propose message")

                new_val, t, c = data.split(' ', 2)
                sndr, rcvr, amount = data.split('-', 2)
                print("t: ", t, " new_val: ", new_val, " c: ", c)
                if config.algorithm == "optimistic":
                    #case on whether inter or intra cluster
                    if self.replicated_val.same_ledger(sndr, rcvr):
                        self.replicated_val.propose_update(new_val, t, c)
                    else:
                        print("inter-ledger transaction initiated - optimistic algorithm")
                        self.replicated_val.propose_update(new_val, t, c)
                        self.send_propose(rcvr, new_val, t, c)
                        #TODO self.send_propose(rcvr, data)
                elif config.algorithm == 'coordinator':
                    if self.replicated_val.double_spending(sndr):
                        print("ERROR: no double spending: must wait until pending commit for next transaction")
                        return

                    #case on whether inter or intra cluster 
                    if self.replicated_val.same_ledger(sndr, rcvr):
                        self.replicated_val.propose_update( new_val, t, c)
                    else:
                        #TODO error checking, move the below case to a coordinator alg function call
                        print("inter-ledger transaction initiated - coordinator based algorithm")
                        lca_addr, lca_id = config.lca[(self.replicated_val.get_cluster(sndr), self.replicated_val.get_cluster(rcvr))]
                        self.replicated_val.new_inter_ledger(data)
                        self.send_propose_to_lca(lca_addr, new_val)
            #optimistic alg
            elif message_type == 'propose_to_rcvr':
                new_val, t, c = data.split(' ', 2)
                self.replicated_val.propose_update(new_val, t, c)
            # coordinator based alg
            elif message_type == 'propose_to_lca' or message_type == 'seq_req' or message_type == 'seq' or message_type == 'lcacommit_c':
                print("rcv: ", packet)
                handler = getattr(self.replicated_val, 'receive_' + message_type, None)
                kwargs = json.loads(data)
                handler(**kwargs)
            #normal case
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
        indx = str(int(self.replicated_val.get_network_uid()[0]) * 1000)
        self.transport.write(text, config.client[indx])

    def send_update(self, proposal_value):
        print('SEND UPDATE UPWARD',': ', proposal_value)
        text = bytes('propose_update {0} {1} {2}'.format(proposal_value, datetime.now().time(), self.replicated_val.get_network_uid()), 'utf-8')
        #text = bytes('propose ' + proposal_value, 'utf-8')
        self.transport.write(text, config.leader[self.replicated_val.get_network_uid()])

    
    #coordinator based alg

    #use this send for sending between layers - insert addr_tuple directly rather than peer_uid
    def _send_c(self, addr_tuple, message_type, **kwargs):
        msg = '{0} {1}'.format(message_type, json.dumps(kwargs))
        print('snd', addr_tuple, ':', msg)
        text = bytes(msg, 'utf-8')
        self.transport.write(text, addr_tuple)

    def send_propose_to_lca(self, addr, proposal):
        # text = bytes('propose_to_lca {0}'.format(proposal), 'utf-8')
        # print('snd to lca: ', text)
        # self.transport.write(text, addr)
        self._send_c(addr, 'propose_to_lca', proposal=proposal)

    def send_seq_req(self, addr, proposal):
        # text = bytes('seq_req {0}'.format(self.replicated_val.get_network_uid()), 'utf-8')
        # print('snd seq req: ', text, ' to: ', addr)
        # self.transport.write(text, addr)
        self._send_c(addr, 'seq_req', lca_id=self.replicated_val.get_network_uid(), proposal=proposal)

    def send_seq(self, addr, seq_num, proposal):
        self._send_c(addr, "seq", seq_num=seq_num, proposal=proposal)

    def send_preprepare_c(self, peer_uid, v, n, m, i):
        #print("---------messenger calling send to :", peer_uid)
        print("-----send_prepare_c: message: ", m)
        self._send(peer_uid, 'preprepare_c', view = v, seq_num = n, message = m, id = i)

    # def _send(self, to_uid, message_type, **kwargs):
    #     msg = '{0} {1}'.format(message_type, json.dumps(kwargs))
    #     print('snd', to_uid, ':', msg)
    #     text = bytes(msg, 'utf-8')
    #     self.transport.write(text, self.addrs[to_uid])


    def send_prepare_c(self, peer_uid, v, n, d, i):
        self._send(peer_uid, 'prepare_c', view = v, seq_num = n, digest = d, id = i)

    def send_commit_c(self, peer_uid, v, n, d, i):
        self._send(peer_uid, 'commit_c', view = v, seq_num = n, digest = d, id = i)

    def send_lcacommit_c(self, addr, seq_num, transaction):
        self._send_c(addr, "lcacommit_c", seq_num = seq_num, transaction = transaction)

    



    # def send_prepare_c(self, peer_uid, instance_number, proposal_id):
    #     self._send(peer_uid, 'prepare_c', instance_number = instance_number,
    #                                     proposal_id     = proposal_id)

    # def send_promise_c(self, peer_uid, instance_number, proposal_id, last_accepted_id, last_accepted_value):
    #     self._send(peer_uid, 'promise_c',  instance_number     = instance_number,
    #                                      proposal_id         = proposal_id,
    #                                      last_accepted_id    = last_accepted_id,
    #                                      last_accepted_value = last_accepted_value )

    # def send_accept_c(self, peer_uid, instance_number, proposal_id, proposal_value):
    #     self._send(peer_uid, 'accept_c', instance_number = instance_number,
    #                                    proposal_id     = proposal_id,
    #                                    proposal_value  = proposal_value)

    # def send_accepted_c(self, peer_uid, instance_number, proposal_id, proposal_value):
    #     self._send(peer_uid, 'accepted_c', instance_number = instance_number,
    #                                      proposal_id     = proposal_id,
    #                                      proposal_value  = proposal_value)

    # def send_commit_c(self, addr, seq_num, transaction):
    #     self._send_c(addr, "commit_c", seq_num = seq_num, transaction = transaction)



    #optimistic alg
    def send_propose(self, to_uid, value, t, c):
        rcvr_leader_addr = config.peers[self.replicated_val.get_height(to_uid)][self.replicated_val.get_cluster(to_uid)][str(self.replicated_val.get_cluster(to_uid)*1000)]
        print(to_uid)
        print(self.replicated_val.get_height(to_uid))
        print(self.replicated_val.get_cluster(to_uid))
        print(rcvr_leader_addr)
        # should t be datetime.now and c be  self.replicated_value.get_netowrk_uid()?
        text = bytes('propose_to_rcvr {0} {1} {2}'.format(value, t, c), 'utf-8')
        self.transport.write(text, rcvr_leader_addr)

    # TODO: add send_reply - when accepted is received by learner, send value to client, client will continually check for messages and ignore those with same seq/proposal num

    # edge mobility
    def send_account(self, addr, node, account):
        self._send_c(addr, 'account', node=node, account=account)

