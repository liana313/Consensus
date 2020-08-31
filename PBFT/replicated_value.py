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

from twisted.internet import reactor, defer, task

from composable_pbft import PbftInstance, PrePrepare, Prepare, Commit, Resolution


class BaseReplicatedValue (object):
    pbft_instance = None

    def __init__(self, network_uid, peers, state_file):
        self.messenger   = None
        self.network_uid = network_uid
        self.id = None
        self.peers       = peers            # list of peer network uids
        #self.quorum_size = len(peers)/2 + 1
        self.num_replicas = len(peers)
        self.state_file  = state_file
        self.prepare_messages = dict(dict(dict()))
        self.requests = dict()


        

        #self.pbft_instance = Replica(self.id, self.num_replicas)
        #print("pbft_instance set")

        self.load_state()
        self.pbft_instance = PbftInstance(self.id, self.num_replicas)
        print("*****initiating self: ", self.id, self.pbft_instance.i)

        # ret = False
        # ret = self.checkVisiblity()
        # if ret == True:
        #     print("good1")

        
        # self.paxos = PaxosInstance(self.network_uid, self.quorum_size,
        #                            self.promised_id, self.accepted_id,
        #                            self.accepted_value)

    # TODO: fix name
    def get_network_uid(self):
        return self.network_uid
    
    def set_messenger(self, messenger):
        self.messenger = messenger

        
    def save_state(self, committed_value):
        self.committed_value = committed_value

        
    def load_state(self):
        self.committed_value = None
        if (self.network_uid == 'A'):
            self.id = 0
        elif (self.network_uid == 'B'):
            self.id = 1
        elif (self.network_uid == 'C'):
            self.id = 2
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
        # msg = (0, new_value, timestamp, client_id)
        # ret = self.pbft_instance.receive_request( msg )
        if ret == True:
            #print("ret is: ", ret)
            ret = self.pbft_instance.send_preprepare(client_request)
            #print("ret is: ", ret)
            if not ret == None and isinstance(ret, PrePrepare):
                #print("sending preprepare")
                self.send_preprepare(ret.seq_num, ret.message)


    def advance_instance(self, new_instance_number, new_current_value, catchup=False):
        self.save_state(new_instance_number, new_current_value, None, None, None)
        
        # TODO - line commented out below to keep samw leader for now
        #self.paxos = PaxosInstance(self.network_uid, self.quorum_size, None, None, None)

        print('UPDATED -- instance_number: ', new_instance_number, 'value: ',new_current_value)


    def send_preprepare(self, seq_num, message):
        if not self.isLeader():
            #print("-----not leader")
            return
        for uid in self.peers:
            if not uid == self.network_uid:
                #print("-----messenger will send to uid :", uid)
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
            print("______RESOLUTION!!!_________")
            #TODO (1) digest should be messege once hashing works
            ret = self.pbft_instance.execute(tuple(digest), view, seq_num)
            print(ret)
            if not ret == None:
                self.messenger.send_reply(ret[5])
            #else:
                #print("execution failure")
    

    

            
    # def receive_accepted(self, from_uid, instance_number, proposal_id, proposal_value):
    #     # Only process messages for the current link in the multi-paxos chain
    #     if instance_number != self.instance_number:
    #         return

    #     m = self.paxos.receive_accepted( Accepted(from_uid, proposal_id, proposal_value) )
        
    #     if isinstance(m, Resolution):
    #         print("----Resolution!!!")
    #         self.messenger.send_reply(self.accepted_value)
    #         self.advance_instance( self.instance_number + 1, proposal_value )
    
