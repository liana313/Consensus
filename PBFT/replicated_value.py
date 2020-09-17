# This module provides a base class for maintaining a single replicated
# value via multi-paxos. The responsibilities of this class are:
#
#    * Loading and saving the state to/from disk
#    * Maintaining the integrity of the multi-paxos chain
#    * Bridging the composable_paxos.PaxosInstance object for the current link
#      in the multi-paoxs chain with the Messenger object used to send and
#      receive messages over the network.
#
# In order to provide clean separation-of-concerns, this class is completely
# passive. Active operations like the logic used to ensure that resolution
# is achieved and catching up after falling behind are left to Mixin classes.

import os
import json
import random
import os.path
import config


import time

from twisted.internet import reactor, defer, task

from composable_pbft import PbftInstance, PrePrepare, Prepare, Commit, Resolution




class BaseReplicatedValue (object):
    pbft_instance = None

    def __init__(self, network_uid, peers, state_file):
        self.messenger   = None
        self.network_uid = network_uid
        self.id = None #TODO remove this variable from code and replace all with network_uid
        self.peers       = peers            # list of peer network uids
        #self.quorum_size = len(peers)/2 + 1
        self.num_replicas = len(peers)
        self.state_file  = state_file
        self.prepare_messages = dict(dict(dict()))
        self.requests = dict()
        self.height = self.get_height(self.network_uid)

        #ledger data structures
        self.temp_updates = [] #list of string proposals waiting to be sent to level above in hieararchy
        self.lastTime = 0 # last time node sent an update to lead proposer 
        self.sendRate = 10 # num millisecs between updates sent to layer above
        #coordinator based data structures
        self.pending_inter_ledger = dict() # self seq_num -> transaction - may not need to use (revise new_inter_ledger fct)
        self.uncommitted_nodes = [] #list of string ids of nodes that have pending inter-ledger transactions (coordinator based)
        self.inter_ledger_seq = dict() #transaction -> seq (a list of seq num, sndr uid) --- stored in lca of coordinator based alg to track if 2 seq nums have been received for a transaction
        self.inter_ledger_transaction = dict() #seq #-> tranasaction - used by lca so that after consensus on seq # it can recover transaction and send back to clusters




        

        #self.pbft_instance = Replica(self.id, self.num_replicas)
        #print("pbft_instance set")

        self.load_state()
        self.pbft_instance = PbftInstance(self.id, self.num_replicas)
        print("*****initiating self: id: ", self.id, ", instance: ", self.pbft_instance.i)

        # ret = False
        # ret = self.checkVisiblity()
        # if ret == True:
        #     print("good1")

        
        # self.paxos = PaxosInstance(self.network_uid, self.quorum_size,
        #                            self.promised_id, self.accepted_id,
        #                            self.accepted_value)

    #cordinator based alg (and also the other inter ledger alg) - call wwhen a new inter ledger transaction becomes initiated and pending
    def new_inter_ledger(self, transaction):
        self.pending_inter_ledger[transaction] = True
        sndr, rcvr, amount = transaction.split('-', 2)
        self.uncommitted_nodes.append(sndr)

    def double_spending(self, network_uid):
        if network_uid in self.uncommitted_nodes:
            return True
        else:
            return False
    
    def send_updates(self, proposal_value):
        #TODO remove config so you don't need to import it here
        #append to temp list of updates waiting
        #case on whether this is a single proposal or ledger
        timeMillisecs = int(round(time.time() * 1000))
        if " " in proposal_value:
            #print("---------------------_REACHED")
            transactions = proposal_value.strip('][').replace('\'', '').split(', ') # returns list of string proposals
            #print("-----send_updates: ", transactions)
            self.temp_updates.extend(transactions)
        else:
            transactions = proposal_value.strip('][').split(', ')
            #print("-----send_updates: ", transactions)
            #print("-----type:", type(transactions))
            #print("-----temp:", self.temp_updates)
            self.temp_updates.append(proposal_value)
        self.handle_time(timeMillisecs)

    def handle_time(self, timeMillisecs) :

        if self.network_uid in config.leader.keys():
            #print("handle time called!!")
            if (timeMillisecs - self.lastTime > self.sendRate):
                #print("temp_updates: ", self.temp_updates)
                if len(self.temp_updates) > 0: 
                    if not " " in str(self.temp_updates):
                        self.messenger.send_update(self.temp_updates.pop())
                    else:
                        self.messenger.send_update(self.temp_updates)
                    self.temp_updates = []
                    self.lastTime = timeMillisecs

    # TODO: fix name
    def get_network_uid(self):
        return self.network_uid

    #get cluster of node with network_uid (string)
    def get_cluster(self, network_uid):
        cluster = 0
        if int(network_uid) == 1 or int(network_uid) == 2 or int(network_uid) == 3:
            cluster = 1
        else:
            cluster = int(network_uid[0])
        return cluster

    def get_height(self, network_uid):
        return (4 - len(network_uid))

    def same_ledger(self, network_uid_a, network_uid_b):
        if self.get_cluster(network_uid_a) == self.get_cluster(network_uid_b) and self.get_height(network_uid_a) == self.get_height(network_uid_b):
            return True
        else: 
            return False
    
    def set_messenger(self, messenger):
        self.messenger = messenger

        
    def save_state(self, committed_value):
        self.committed_value = committed_value

        
    def load_state(self):
        self.committed_value = None
        if self.network_uid in config.cluster31:
            self.id = int(str(int(self.network_uid) - 1))
        else:
            self.id = int(self.network_uid[len(self.network_uid) - 1])
        # if (self.network_uid == 'A'):
        #     self.id = 0
        # elif (self.network_uid == 'B'):
        #     self.id = 1
        # elif (self.network_uid == 'C'):
        #     self.id = 2
        #self.pbft_instance = Replica(self.id, self.num_replicas)

    def isLeader(self):
        return self.id == self.pbft_instance.view_i % self.num_replicas

    # def checkVisiblity(self):
    #     print("visibilyt check")
    #     ret = False
    #     #ret = self.pbft_instance.has_new_view(0)
    #     ret = self.pbft_instance.receive_request( 0 )
    #     return ret

    def propose_update(self, new_value, timestamp, client_id):
        """
        This is a key method that some of the mixin classes override in order
        to provide additional functionality when new values are proposed
        """
        # this happens when client sends propose message to server, and received by messenger of replicated_value
        # ret = False
        # print("ret is false, going to check Visiblity")
        # ret = self.checkVisiblity()
        # if ret == True:
        #     print("good2")
        # else:
        #     print("bad2")
        self.requests[new_value] = (timestamp, client_id)
        client_request = (self.pbft_instance._REQUEST, new_value, timestamp, client_id)
        ret = self.pbft_instance.receive_request( client_request )
        print("reached in propose_update")
        print("client request: ", client_request)
        # msg = (0, new_value, timestamp, client_id)
        # ret = self.pbft_instance.receive_request( msg )
        if ret == True:
            #print("ret is: ", ret)
            ret = self.pbft_instance.send_preprepare(client_request)
            #print("ret is: ", ret)
            if not ret == None and isinstance(ret, PrePrepare):
                print("sending preprepare")
                self.send_preprepare(ret.seq_num, ret.message)


    def advance_instance(self, new_instance_number, new_current_value, catchup=False):
        self.save_state(new_instance_number, new_current_value, None, None, None)
        
        # TODO - line commented out below to keep samw leader for now
        #self.paxos = PaxosInstance(self.network_uid, self.quorum_size, None, None, None)

        print('UPDATED -- instance_number: ', new_instance_number, 'value: ',new_current_value)


    def send_preprepare(self, seq_num, message):
        if not self.isLeader():
            print("-----not leader")
            return
        for uid in self.peers:
            if not uid == self.network_uid:
                print("-----messenger will send to uid :", uid)
                self.messenger.send_preprepare(uid, self.pbft_instance.view_i, seq_num, message, self.id)
    
    def send_prepare(self, seq_num, digest):
        for uid in self.peers:
            self.messenger.send_prepare(uid, self.pbft_instance.view_i, seq_num, digest, self.id)

    def send_commit(self, seq_num, digest):
        for uid in self.peers:
            self.messenger.send_commit(uid, self.pbft_instance.view_i, seq_num, digest, self.id)

    def receive_preprepare(self, from_uid, view, seq_num, message, id):
        #TODO
        #prepr = (r1._PREPREPARE, 0, 1, request, 0)
        ret = self.pbft_instance.receive_preprepare( (self.pbft_instance._PREPREPARE, view, seq_num, tuple(message), id) )
        if not ret == None and isinstance(ret, Prepare):
            #self.save_state cal TODO
            #print("------receive_preprepare: sending prepare")
            self.send_prepare(seq_num, ret.digest)
            #self.prepare_messages[ret.digest][view][seq_num] = message #TODO (1) see TODO (1) in receive_prepare

    def receive_prepare(self, from_uid, view, seq_num, digest, id):
        #TODO
        self.pbft_instance.receive_prepare((self.pbft_instance._PREPARE, view, seq_num, tuple(digest), id))
        #m = self.prepare_messages[tuple(digest)][view][seq_num] TODO (1): need to creat mapping once hash is working and arg for prepared should be m not digest
        if(self.pbft_instance.prepared(tuple(digest), view, seq_num)):
            #print("------receive_prepare: prepared")
            #self.prepared_message = d
            self.send_commit(seq_num, digest)

    def receive_commit(self, from_uid, view, seq_num, digest, id):
        #TODO
        self.pbft_instance.receive_commit((self.pbft_instance._COMMIT, view, seq_num, tuple(digest), id))
        #TODO (1) see TODO (1) in receive_prepare - need to replace committed's first arg with message not digest
        if(self.pbft_instance.committed(tuple(digest), view, seq_num)):
            #TODO (1) digest should be messege once hashing works
            ret = self.pbft_instance.execute(tuple(digest), view, seq_num)
            if not ret == None:
                print("______RESOLUTION!!!_________")
                print(ret)
                if self.height == 0:
                    result = self.pbft_instance.update_ledger(ret[5], True)
                    self.messenger.send_reply(ret[5])

                else:
                    result = self.pbft_instance.update_ledger(ret[5], False)
                self.send_updates(ret[5])
                #TODO - advance instances
                
            #else:
                #print("old")
    

    #Coordinator based algorithm
    def receive_propose_to_lca(self, proposal):
        sndr, rcvr, amount = proposal.split('-', 2)
        if not self.get_height(sndr) == 0 or not self.get_height(rcvr) == 0:
            print("------error in receive_propose_to_lca: inter-ledger transaction not at leaf level") 
            return
        sndr_cluster = self.get_cluster(sndr)
        rcvr_cluster = self.get_cluster(rcvr)
        sndr_leader_addr = config.peers[0][sndr_cluster][str(sndr_cluster*1000)]
        rcvr_leader_addr = config.peers[0][rcvr_cluster][str(rcvr_cluster*1000)]
        self.send_seq_req(sndr_leader_addr, proposal)
        self.send_seq_req(rcvr_leader_addr, proposal)

    def send_seq_req(self, to_addr, proposal):
        self.messenger.send_seq_req(to_addr, proposal)


    def receive_seq_req(self, lca_id, proposal):
        print("----received seq request")
        # get next seq num
        seq_num = self.pbft_instance.next_seq_num()
        # TODO store seq num and transaction mapping ?
        #send seq num back to lca
        to_addr = config.peers[self.get_height(lca_id)][self.get_cluster(lca_id)][lca_id]
        self.messenger.send_seq(to_addr, seq_num, proposal)

    def receive_seq(self, seq_num, proposal):
        print("-----received seq num")
        #remember seq_nums are lists right now of form [seq num, sndr's uid]
        if proposal in self.inter_ledger_seq.keys():
            seq_1 = self.inter_ledger_seq[proposal]
            #remove proposal now that we have two seq_nums
            del self.inter_ledger_seq[proposal]
            seq_2 = seq_num
            new_seq = '{0}-{1}'.format(seq_1, seq_2)

            print("new seq: ", new_seq)
            self.inter_ledger_transaction[new_seq] = proposal
            self.propose_update_c(new_seq, time.time(), self.get_network_uid())
        else:
            self.inter_ledger_seq[proposal] = seq_num

    #Coordinator based algorithm - consensus functions for lca's cluster      
    def propose_update_c(self, new_value, timestamp, client_id):
        """
        This is a key method that some of the mixin classes override in order
        to provide additional functionality when new values are proposed
        """
        # this happens when client sends propose message to server, and received by messenger of replicated_value
        # ret = False
        # print("ret is false, going to check Visiblity")
        # ret = self.checkVisiblity()
        # if ret == True:
        #     print("good2")
        # else:
        #     print("bad2")
        self.requests[new_value] = (timestamp, client_id)
        client_request = (self.pbft_instance._REQUEST, new_value, timestamp, client_id)
        ret = self.pbft_instance.receive_request( client_request )
        #print("reached in propose_update")
        #print("client request: ", client_request)
        # msg = (0, new_value, timestamp, client_id)
        # ret = self.pbft_instance.receive_request( msg )
        if ret == True:
            #print("ret is: ", ret)
            ret = self.pbft_instance.send_preprepare(client_request)
            #print("ret is: ", ret)
            if not ret == None and isinstance(ret, PrePrepare):
                print("sending preprepare")
                self.send_preprepare_c(ret.seq_num, ret.message)

    def send_preprepare_c(self, seq_num, message):
        if not self.isLeader():
            print("-----not leader")
            return
        for uid in self.peers:
            if not uid == self.network_uid:
                print("-----messenger will send to uid :", uid)
                self.messenger.send_preprepare_c(uid, self.pbft_instance.view_i, seq_num, message, self.id)

    def receive_preprepare_c(self, from_uid, view, seq_num, message, id):
        #TODO
        #prepr = (r1._PREPREPARE, 0, 1, request, 0)
        ret = self.pbft_instance.receive_preprepare( (self.pbft_instance._PREPREPARE, view, seq_num, tuple(message), id) )
        if not ret == None and isinstance(ret, Prepare):
            #self.save_state cal TODO
            #print("------receive_preprepare: sending prepare")
            self.send_prepare_c(seq_num, ret.digest)
            #self.prepare_messages[ret.digest][view][seq_num] = message #TODO (1) see TODO (1) in receive_prepare

    def send_prepare_c(self, seq_num, digest):
        for uid in self.peers:
            self.messenger.send_prepare_c(uid, self.pbft_instance.view_i, seq_num, digest, self.id)
    
    def receive_prepare_c(self, from_uid, view, seq_num, digest, id):
        #TODO
        self.pbft_instance.receive_prepare((self.pbft_instance._PREPARE, view, seq_num, tuple(digest), id))
        #m = self.prepare_messages[tuple(digest)][view][seq_num] TODO (1): need to creat mapping once hash is working and arg for prepared should be m not digest
        if(self.pbft_instance.prepared(tuple(digest), view, seq_num)):
            #print("------receive_prepare: prepared")
            #self.prepared_message = d
            self.send_commit_c(seq_num, digest)

    def send_commit_c(self, seq_num, digest):
        for uid in self.peers:
            self.messenger.send_commit_c(uid, self.pbft_instance.view_i, seq_num, digest, self.id)


    def leaf_cluster_addrs(self, cluster):
        yield config.peers[0][cluster][str(cluster*1000)]
        yield config.peers[0][cluster][str(cluster*1000 + 1)]
        yield config.peers[0][cluster][str(cluster*1000 + 2)]
        yield config.peers[0][cluster][str(cluster*1000 + 3)]


    def receive_commit_c(self, from_uid, view, seq_num, digest, id):
        #TODO
        self.pbft_instance.receive_commit((self.pbft_instance._COMMIT, view, seq_num, tuple(digest), id))
        #TODO (1) see TODO (1) in receive_prepare - need to replace committed's first arg with message not digest
        if(self.pbft_instance.committed(tuple(digest), view, seq_num)):
            #TODO (1) digest should be messege once hashing works
            ret = self.pbft_instance.execute(tuple(digest), view, seq_num)
            if not ret == None:
                print("______RESOLUTION!!!_________")
                #digest: [_REQUEST, seqnum, datetime, sndr id]
                new_seq_num = digest[1]
                if new_seq_num in self.inter_ledger_transaction:
                    # in lead proposer of cluster
                    transaction = self.inter_ledger_transaction[new_seq_num]  
                    del self.inter_ledger_transaction[new_seq_num]
                    #determine involved clusters, and their lead proposers
                    sndr, rcvr, amount = transaction.split('-', 2)
                    sndr_cluster = self.get_cluster(sndr)
                    rcvr_cluster = self.get_cluster(rcvr)
                    for addr in self.leaf_cluster_addrs(sndr_cluster):
                        self.messenger.send_lcacommit_c(addr, new_seq_num, transaction)
                    for addr in self.leaf_cluster_addrs(rcvr_cluster):
                        self.messenger.send_lcacommit_c(addr, new_seq_num, transaction)
                # print(ret)
                # if self.height == 0:
                #     result = self.pbft_instance.update_ledger(ret[5], True)
                #     self.messenger.send_reply(ret[5])

                # else:
                #     result = self.pbft_instance.update_ledger(ret[5], False)
                # self.send_updates(ret[5])

    def receive_lcacommit_c(self, seq_num, transaction):
        print("received lcacommit")
        #update data structures so that sndr can now transact again
        if transaction in self.pending_inter_ledger.keys():
            del self.pending_inter_ledger[transaction]
        sndr, rcvr, amount = transaction.split('-', 2)
        if sndr in self.uncommitted_nodes:
            self.uncommitted_nodes.remove(sndr)
        #update ledger
        result = self.pbft_instance.update_ledger(transaction, True)
        print(result)
        self.send_updates(transaction)
            
    # def receive_accepted(self, from_uid, instance_number, proposal_id, proposal_value):
    #     # Only process messages for the current link in the multi-paxos chain
    #     if instance_number != self.instance_number:
    #         return

    #     m = self.paxos.receive_accepted( Accepted(from_uid, proposal_id, proposal_value) )
        
    #     if isinstance(m, Resolution):
    #         print("----Resolution!!!")
    #         self.messenger.send_reply(self.accepted_value)
    #         self.advance_instance( self.instance_number + 1, proposal_value )
    
