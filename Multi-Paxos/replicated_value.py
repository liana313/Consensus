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
import time
import config

from twisted.internet import reactor, defer, task

from composable_paxos import PaxosInstance, ProposalID, Prepare, Nack, Promise, Accept, Accepted, Resolution


class BaseReplicatedValue (object):

    def __init__(self, network_uid, peers, state_file):
        self.messenger   = None
        self.network_uid = network_uid
        self.peers       = peers            # list of peer network uids
        self.quorum_size = len(peers)/2 + 1
        self.state_file  = state_file
        self.lastTime = 0 # last time node sent an update to lead proposer 
        self.sendRate = 10 # num millisecs between updates sent to layer above
        self.height = self.get_height(self.network_uid)
        self.temp_updates = [] #list of string proposals waiting to be sent to level above in hieararchy

        #coordinator based data structures
        self.pending_inter_ledger = dict() # self seq_num -> transaction - may not need to use (revise new_inter_ledger fct)
        self.uncommitted_nodes = [] #list of string ids of nodes that have pending inter-ledger transactions (coordinator based)
        self.inter_ledger_seq = dict() #transaction -> seq (a list of seq num, sndr uid) --- stored in lca of coordinator based alg to track if 2 seq nums have been received for a transaction
        self.inter_ledger_transaction = dict() #seq #-> tranasaction - used by lca so that after consensus on seq # it can recover transaction and send back to clusters



        self.load_state()

        self.paxos = PaxosInstance(self.network_uid, self.quorum_size,
                                   self.promised_id, self.accepted_id,
                                   self.accepted_value)


    #coordinator based alg (and also the other inter ledger alg) - call wwhen a new inter ledger transaction becomes initiated and pending
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
            # else:
            #     #append to temp list of updates waiting
            #     #case on whether this is a single proposal or ledger
            #     if proposal_value.contains(" "):
            #         transactions = proposal_value.strip('][').split(', ') # returns list of string proposals
            #         self.temp_updates.append(transactions)
            #     else:
            #         self.temp_updates.append(proposal_value)


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

        
    def save_state(self, instance_number, current_value, promised_id, accepted_id, accepted_value):
        '''
        For crash recovery purposes, Paxos requires that some state be saved to
        persistent media prior to sending Promise and Accepted messages. We'll
        also save the state of the multi-paxos chain here so everything is kept
        in one place.
        '''
        self.instance_number = instance_number
        self.current_value   = current_value
        self.promised_id     = promised_id
        self.accepted_id     = accepted_id
        self.accepted_value  = accepted_value

        # tmp = self.state_file + '.tmp'
        
        # with open(tmp, 'w') as f:
        #     f.write( json.dumps( dict(instance_number = instance_number,
        #                               promised_id     = promised_id,
        #                               accepted_id     = accepted_id,
        #                               accepted_value  = accepted_value,
        #                               current_value   = current_value) ) )
        #     f.flush()
        #     os.fsync(f.fileno()) # Wait for the data to be written to disk
            
        # # os.rename() is an atomic filesystem operation. By writing the new
        # # state to a temporary file and using this method, we avoid the potential
        # # for leaving a corrupted state file in the event that a crash/power loss
        # # occurs in the middle of update.
        # os.rename(tmp, self.state_file)

        
    def load_state(self):
        self.instance_number = 0
        self.current_value = None
        self.promised_id = None
        self.accepted_id = None
        self.accepted_value = None
        # if not os.path.exists(self.state_file):
        #     with open(self.state_file, 'w') as f:
        #         f.write( json.dumps( dict(instance_number = 0,
        #                                   promised_id     = None,
        #                                   accepted_id     = None,
        #                                   accepted_value  = None,
        #                                   current_value   = None) ) )
        #         f.flush()

        # with open(self.state_file) as f:
        #     m = json.loads(f.read())

        #     def to_pid(v):
        #         return ProposalID(*v) if v else None

        #     self.instance_number = m['instance_number']
        #     self.current_value   = m['current_value']
        #     self.promised_id     = to_pid(m['promised_id'])
        #     self.accepted_id     = to_pid(m['accepted_id'])
        #     self.accepted_value  = m['accepted_value']
        
    
    def propose_update(self, new_value):
        """
        This is a key method that some of the mixin classes override in order
        to provide additional functionality when new values are proposed
        """
        # this happens when client sends propose message to server, and received by messenger of replicated_value
        if self.paxos.proposed_value is None:
            self.paxos.propose_value( new_value, False )
        #if self.instance_number > 0:



    def advance_instance(self, new_instance_number, new_current_value, catchup=False):
        self.save_state(new_instance_number, new_current_value, None, None, None)
        
        # TODO - line commented out below to keep samw leader for now
        #self.paxos = PaxosInstance(self.network_uid, self.quorum_size, None, None, None)

        print('UPDATED -- instance_number: ', new_instance_number, 'value: ',new_current_value)


    def send_prepare(self, proposal_id):
        for uid in self.peers:
            self.messenger.send_prepare(uid, self.instance_number, proposal_id)


    def send_accept(self, proposal_id, proposal_value):
        for uid in self.peers:
            self.messenger.send_accept(uid, self.instance_number, proposal_id, proposal_value)

            
    def send_accepted(self, proposal_id, proposal_value):
        for uid in self.peers:
            self.messenger.send_accepted(uid, self.instance_number, proposal_id, proposal_value)

    def receive_prepare(self, from_uid, instance_number, proposal_id):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return
        
        m = self.paxos.receive_prepare( Prepare(from_uid, proposal_id) )
        
        if isinstance(m, Promise):
            self.save_state(self.instance_number, self.current_value, m.proposal_id, m.last_accepted_id, m.last_accepted_value)
            
            self.messenger.send_promise(from_uid, self.instance_number,
                                        m.proposal_id, m.last_accepted_id, m.last_accepted_value )
        else:
            self.messenger.send_nack(from_uid, self.instance_number, proposal_id, self.promised_id)


    def receive_nack(self, from_uid, instance_number, proposal_id, promised_proposal_id):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return

        self.paxos.receive_nack( Nack(from_uid, self.network_uid, proposal_id, promised_proposal_id) )
        
            
    def receive_promise(self, from_uid, instance_number, proposal_id, last_accepted_id, last_accepted_value):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return

        m = self.paxos.receive_promise( Promise(from_uid, self.network_uid, proposal_id,
                                                last_accepted_id, last_accepted_value) )
        
        if isinstance(m, Accept):
            self.send_accept(m.proposal_id, m.proposal_value)

            
    def receive_accept(self, from_uid, instance_number, proposal_id, proposal_value):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return

        m = self.paxos.receive_accept( Accept(from_uid, proposal_id, proposal_value) )
        
        if isinstance(m, Accepted):
            self.save_state(self.instance_number, self.current_value, self.promised_id, proposal_id, proposal_value)
            self.send_accepted(m.proposal_id, m.proposal_value)
        else:
            self.messenger.send_nack(from_uid, self.instance_number, proposal_id, self.promised_id)

            
    def receive_accepted(self, from_uid, instance_number, proposal_id, proposal_value):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return

        m = self.paxos.receive_accepted( Accepted(from_uid, proposal_id, proposal_value) )
        
        if isinstance(m, Resolution):
            print("----Resolution!!!")
            if self.height == 0:
                result = self.paxos.update_ledger(proposal_value, True)
            else:
                result = self.paxos.update_ledger(proposal_value, False)
            
            self.send_updates(proposal_value) #send update if last update was more than 10millisec ago
            if self.height == 0:
                self.messenger.send_reply(result)
            #self.messenger.send_reply(self.accepted_value)
            self.advance_instance( self.instance_number + 1, proposal_value )

    #Coordinator based algorithm
    def receive_propose_to_lca(self, proposal):
        sndr, rcvr, amount = proposal.split('-', 2)
        if not self.get_height(sndr) == 0 or not self.get_height(rcvr) == 0:
            print("------error in receive_propose_to_lca: inter-ledger transaction not at leaf level") 
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
        seq_num = self.paxos.next_seq_num()
        # TODO store seq num and transaction mapping ?
        #send seq num back to lca
        to_addr = config.peers[self.get_height(lca_id)][self.get_cluster(lca_id)][lca_id]
        self.messenger.send_seq(to_addr, seq_num, proposal)

    def receive_seq(self, seq_num, proposal):
        print("-----received seq num")
        #remember seq_nums are lists right now of form [seq num, sndr's uid]
        if proposal in self.inter_ledger_seq.keys():
            seq_1 = self.inter_ledger_seq[proposal][0]
            #remove proposal now that we have two seq_nums
            del self.inter_ledger_seq[proposal]
            seq_2 = seq_num[0]
            new_seq = '{0}-{1}'.format(seq_1, seq_2)

            print("new seq: ", new_seq)
            self.inter_ledger_transaction[new_seq] = proposal
            self.propose_update_c(new_seq)
        else:
            self.inter_ledger_seq[proposal] = seq_num


    #Coordinator based algorithm - consensus functions for lca's cluster
    def propose_update_c(self, new_value):
        # check whether leader is steady and prev sent prepare
        if self.paxos.proposed_value is None:
            self.paxos.propose_value( new_value, False )
        if (self.instance_number > 0):
            print("skipping prepare")
            self.paxos.propose_value( new_value, True)
            m = self.paxos.accept() # skip sending prepare and waiting for promise
            self.send_accept_c(m.proposal_id, m.proposal_value)
        else:
            m = self.paxos.prepare() # Advances to the next proposal number
            self.send_prepare_c(m.proposal_id) 

    def send_prepare_c(self, proposal_id):
        for uid in self.peers:
            self.messenger.send_prepare_c(uid, self.instance_number, proposal_id)

    def receive_prepare_c(self, from_uid, instance_number, proposal_id):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return
        
        m = self.paxos.receive_prepare( Prepare(from_uid, proposal_id) )
        
        if isinstance(m, Promise):
            self.save_state(self.instance_number, self.current_value, m.proposal_id, m.last_accepted_id, m.last_accepted_value)
            
            self.messenger.send_promise_c(from_uid, self.instance_number,
                                        m.proposal_id, m.last_accepted_id, m.last_accepted_value )
        else:
            self.messenger.send_nack(from_uid, self.instance_number, proposal_id, self.promised_id)

    def receive_promise_c(self, from_uid, instance_number, proposal_id, last_accepted_id, last_accepted_value):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return

        m = self.paxos.receive_promise( Promise(from_uid, self.network_uid, proposal_id,
                                                last_accepted_id, last_accepted_value) )
        
        if isinstance(m, Accept):
            self.send_accept_c(m.proposal_id, m.proposal_value)

    def send_accept_c(self, proposal_id, proposal_value):
        for uid in self.peers:
            self.messenger.send_accept_c(uid, self.instance_number, proposal_id, proposal_value)

    def receive_accept_c(self, from_uid, instance_number, proposal_id, proposal_value):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return

        m = self.paxos.receive_accept( Accept(from_uid, proposal_id, proposal_value) )
        
        if isinstance(m, Accepted):
            self.save_state(self.instance_number, self.current_value, self.promised_id, proposal_id, proposal_value)
            self.send_accepted_c(m.proposal_id, m.proposal_value)
        else:
            self.messenger.send_nack(from_uid, self.instance_number, proposal_id, self.promised_id)

    def send_accepted_c(self, proposal_id, proposal_value):
        for uid in self.peers:
            self.messenger.send_accepted_c(uid, self.instance_number, proposal_id, proposal_value)

    def leaf_cluster_addrs(self, cluster):
        yield config.peers[0][cluster][str(cluster*1000)]
        yield config.peers[0][cluster][str(cluster*1000 + 1)]
        yield config.peers[0][cluster][str(cluster*1000 + 2)]

    def receive_accepted_c(self, from_uid, instance_number, proposal_id, proposal_value):
        # Only process messages for the current link in the multi-paxos chain
        if instance_number != self.instance_number:
            return

        m = self.paxos.receive_accepted( Accepted(from_uid, proposal_id, proposal_value) )
        
        if isinstance(m, Resolution):
            print("----Resolution!!!")
            #get transaction from dict based on seq num (proposal_value) and then remove from dict
            if proposal_value in self.inter_ledger_transaction:
                # in lead proposer of cluster
                transaction = self.inter_ledger_transaction[proposal_value]  
                del self.inter_ledger_transaction[proposal_value]
                #determine involved clusters, and their lead proposers
                sndr, rcvr, amount = transaction.split('-', 2)
                sndr_cluster = self.get_cluster(sndr)
                rcvr_cluster = self.get_cluster(rcvr)
                for addr in self.leaf_cluster_addrs(sndr_cluster):
                    self.messenger.send_commit_c(addr, proposal_value, transaction)
                for addr in self.leaf_cluster_addrs(rcvr_cluster):
                    self.messenger.send_commit_c(addr, proposal_value, transaction)
            #advance instance
            self.advance_instance( self.instance_number + 1, proposal_value )
    
    

    def receive_commit_c(self, seq_num, transaction):
        print("received commti")
        #update data structures so that sndr can now transact again
        if transaction in self.pending_inter_ledger.keys():
            del self.pending_inter_ledger[transaction]
        sndr, rcvr, amount = transaction.split('-', 2)
        if sndr in self.uncommitted_nodes:
            self.uncommitted_nodes.remove(sndr)
        #update ledger
        result = self.paxos.update_ledger(transaction, True)
        print(result)
        self.send_updates(transaction)




            


    
    
