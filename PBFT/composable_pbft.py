# Implements a core pBFT replica as a state machine. 
# Follows the formal specification at: 
# https://www.microsoft.com/en-us/research/wp-content/uploads/2017/01/tm590.pdf

from collections import defaultdict
import collections
from hashlib import sha256
from collections import Counter
from datetime import datetime

from datetime import time


NoneT = lambda: None

class PbftMessage (object):
    '''
    Base class for all messages defined in this module
    '''
    from_uid = None # Set by subclass constructor

    
class PrePrepare (PbftMessage):
    '''
    Preprepare message shoud be multicast to all other replicas from primary
    '''
    def __init__(self, v, n, m, i):
        self.view = v
        self.seq_num = n
        self.message = m
        self.from_uid = i

class Prepare(PbftMessage):
    '''
    Prepare mesage should be multicast to all other replicas if preprepare is accepted
    '''
    def __init__(self, v, n, d, i):
        self.view = v
        self.seq_num = n
        self.digest = d
        self.from_uid = i

class Commit(PbftMessage):
    '''
    Commit message should be multicast to all other replicas when prepared boolean becomes true
    '''
    def __init__(self, v, n, d, i):
        self.view = v
        self.seq_num = n
        self.digest = d
        self.from_uid = i
        
        
class Resolution (PbftMessage):
    '''
    Optional message used to indicate that the final value has been selected
    '''
    def __init__(self, from_uid, value):
        self.from_uid = from_uid
        self.value    = value

        
class InvalidMessageError (Exception):
    '''
    Thrown if a PbftMessage subclass is passed to a class that does not
    support it
    '''

    
class MessageHandler (object):

    def receive(self, msg):
        '''
        Message dispatching function. This function accepts any PbftMessage subclass and calls
        the appropriate handler function
        '''
        handler = getattr(self, 'receive_' + msg.__class__.__name__.lower(), None)
        if handler is None:
            raise InvalidMessageError('Receiving class does not support messages of type: ' + msg.__class__.__name__)
        return handler( msg )



def _C(cond, msg):
    if not cond:
        print(msg)
    return cond

class Replica(MessageHandler):

    _PREPREPARE = "_PREPREPARE" # 1000
    _PREPARE    = "_PREPARE" # 1001
    _REPLY      = "_REPLY" # 1002
    _REQUEST    = "_REQUEST" # 1003
    _COMMIT     = "_COMMIT" # 1004
    _VIEWCHANGE     = "_VIEWCHANGE" # 1005
    _NEWVIEW    = "_NEWVIEW"

    # Checkpoint messages
    _CHECKPOINT = "_CHECKPOINT"

    def filter_type(self, xtype, M=None):
        if M is None:
            M = self.in_i

        for msg in M:
            #print("------------filter_type: msg: ", msg)
            if msg[0] == xtype:
                yield msg


    def __init__(self,i, R):
        self.i = i #id of self
        self.R = R # total number of replicas
        self.f = (R - 1) // 3 #max number of faulty replicas
        self.vali = None # v_0
        self.view_i = 0
        self.in_i = set() # in messages log

        self.out_i = set() # out messages log
        self.last_rep_i = defaultdict(NoneT) #dic from i -> last message to i
        self.last_rep_ti = defaultdict(datetime.time)
        self.seqno_i = 0
        self.last_exec_i = 0
        self.current_value = None

        #ledger data structures
        self.ledger = []
        self.pending_inter_ledger = [] #temporarily stores a cross-ledger transaction so that parent can wait to receive both commtis before committing transaction to its ledger
        self.accounts = dict()
        # TODO: instead of hardcoding, loop over peers by importing config
        self.accounts['1000'] = 1000
        self.accounts['1001'] = 1000
        self.accounts['1002'] = 1000
        self.accounts['1003'] = 1000
        self.accounts['2000'] = 1000
        self.accounts['2001'] = 1000
        self.accounts['2002'] = 1000
        self.accounts['2003'] = 1000
        self.accounts['3000'] = 1000
        self.accounts['3001'] = 1000
        self.accounts['3002'] = 1000
        self.accounts['3003'] = 1000
        self.accounts['4000'] = 1000
        self.accounts['4001'] = 1000
        self.accounts['4002'] = 1000
        self.accounts['4003'] = 1000
        self.transactions = []

        # Initialize checkpoints
        initial_checkpoint = self.to_checkpoint(self.vali, self.last_rep_i, self.last_rep_ti)

        self.checkpts_i = set([(0, initial_checkpoint)])
        for i in range(self.R):
            self.in_i.add( (self._CHECKPOINT, self.view_i, self.last_exec_i, initial_checkpoint, i) )

        # Utility functions

        # Consts
        self.max_out = 30
        self.chkpt_int = 10
        assert self.chkpt_int < self.max_out

        # Testing 
        self.stable_n()
        self.stat = Counter()

    def update_account(self, uid, new_val):
        self.accounts[uid] = new_val    
    
    def next_seq_num(self):
        self.seqno_i = self.seqno_i + 1
        return self.seqno_i

    def to_checkpoint(self, vi, rep, rep_t):
        rep_ser = tuple(sorted(rep.items()))
        rep_t_ser = tuple(sorted(rep_t.items()))

        return (vi, rep_ser, rep_t_ser)

    def from_checkpoint(self, chkpt):
        vali, rep_s, rep_t_s = chkpt
        last_rep_i = defaultdict(NoneT).update(rep_s)
        last_rep_ti = defaultdict(int).update(rep_t_s)

        return (vali, last_rep_i, last_rep_ti)

    def valid_sig(self, i, m):
        return True


    def primary(self, v=None):
        if v is None:
            v = self.view_i
        return v % self.R


    def in_v(self, v):
        return self.view_i == v

    def in_w(self, n):
        return 0 < n - self.stable_n() < self.max_out

    def in_wv(self, v, n):
        return self.in_v(v) and self.in_w(n)

    def stable_n(self):
        min_n = min(n for n,_ in self.checkpts_i)
        return min_n

    def stable_chkpt(self):
        vx = min((n,v) for (n,v) in self.checkpts_i)[1]
        return vx


    def has_new_view(self, v):
        if v == 0:
            return True
        else:
            for msg in self.filter_type(self._NEWVIEW):
                if msg[1] == v:
                    return True
            return False

    def take_chkpt(self, n):
        return (n % self.chkpt_int) == 0


    def hash(self, m, cache={}):
        # if m in cache:
        #     return cache[m]
        # t = ("%2.2f" % m[2]).encode("utf-8")
        # bts = m[1] + b"||" + t + b"||" + m[3] # TODO: fix formatting
        # h = sha256(bts).hexdigest()

        # if len(cache) > 1000:
        #     cache.clear()
        # cache[m] = h
        # return h
        return m


    def prepared(self, m, v, n, M=None):
        #print("-----prepared: entered")
        if M is None:
            #print("-----prepared: M set to self.in_i")
            M = self.in_i

        cond = (self._PREPREPARE, v, n, m, self.primary(v)) in M
        #print("-----prepared: cond: ", cond)
        
        others = set()
        hm = self.hash(m)
        for mx in self.filter_type(self._PREPARE, M): 
            #print("-----prepared: mx: ", mx)
            if mx[:4] == (self._PREPARE, v, n, hm):
                if mx[4] != self.primary(v):
                    others.add(mx[4])

        cond &= len(others) >= 2*self.f
        #print("-----prepared: self.f: ", self.f)
        #print("-----prepared: cond: ", cond)
        return cond


    def committed(self, m, v, n, M=None):
        if M is None:
            M = self.in_i

        cond = False
        for mx in self.filter_type(self._PREPREPARE, M):    #checks there was a preprepare from primary that matches m,v,n
            (_, vp, np, mp, jp) = mx
            cond |= (np, mp) == (n, m) and (jp == self.primary(vp))
        #print("------committed: cond: ", cond)
        cond |= m in M
        #print("------committed: cond: ", cond)
        others = set()
        hm = self.hash(m)
        for mx in M: 
            if mx[:4] == (self._COMMIT, v, n, hm): #vndj
                others.add(mx[4])
        #print("------committed: others len: ", len(others))
        #print("------committed: others: ", others)
        #print("------committed: f: ", self.f)
        cond &= len(others) >= 2*self.f + 1
        #execute(m, v, n)
        return cond


    def correct_view_change(self, msg, v, j):
        (_, _, n, s, C, P, xj) = msg
        # TODO: Check correctness (suspect missing cases)

        ret = True
        ret &= j == xj
        ret &= C == self.compute_C(n, s, C)
        ret &= len(C) > self.f
        ret &= P == self.compute_P(v, P)
    
        ret &= all(np - n <= self.max_out for (_, _, np, _, _) in P )

        return ret


    # Input transactions

    def receive_request(self, msg):
        #print("------reached receive request")
        (_, o, t, c) = msg #operation o, timestamp t, client c
        #print("------t: ", t, " new_val: ", o, " c: ", c)

        # We have already replied to the message
        if c in self.last_rep_ti and t == self.last_rep_ti[c]:
            new_reply = (self._REPLY, self.view_i, t, c, self.i, self.last_rep_i[c])
            self.out_i.add( new_reply )
            #print("----already replied to message)")
            return False
        else:
            #print("----receive_request else entered")
            self.in_i.add( msg )
            #print("----primary: ", self.primary())
            ##print("---i: ", self.i)
            print(self.primary() == self.i)
            # If not the primary, send message to all.
            if self.primary() != self.i:
                #print("----receive_request not primary")
                self.out_i.add( msg )

            ## Liveness hack. TODO: check it.
            else: 
                # If we are the primary, and have send a 
                # preprepare message for this request, send it
                # again here.

                for xmsg in self.filter_type(self._PREPREPARE, self.in_i):
                    if xmsg[1] == self.view_i and \
                       xmsg[3] == msg:

                       self.out_i.add(xmsg)
            return True


    def receive_preprepare(self, msg):
        (_, v, n, m, j) = msg
        #print("------v: ", v, " n: ", n, " m: ", m, " i: ", j)
        if j == self.i: return

        cond = (self.primary() == j)
        #print("------receive_prepr: cond: ", cond)
        cond &= self.in_wv(v, n)
        #print("------receive_prepr: cond: ", cond)
        cond &= self.has_new_view(v)
        #print("------receive_prepr: cond: ", cond)

        hm = self.hash(m)
        #print("------receive_prepare: hash complete")
        for mx in self.filter_type(self._PREPARE):
            (_, vp, np, dp, ip) = mx
            if (vp, np, ip) == (v, n, self.i):
                cond &= (dp == hm)
        #print("------receive_prepr: cond: ", cond)
        if cond:
            # Send a prepare message
            p = (self._PREPARE, v, n, self.hash(m), self.i)
            self.in_i |= set([p, msg])
            self.out_i.add(p)
            return Prepare(v, n, self.hash(m), self.i)

        else:
            # Add the request to the received messages
            if m != None:
                self.in_i.add(m)
            return None


    def receive_prepare(self, msg):
        (_, v, n, d, j) = msg
        if j == self.i: return

        #TODO: why do we need j != primary??? - because this says that message is not from primary 
        #(eg we dont want primary to send preprepare and then also send prepares, it should be
        #primary sends preeprepares, then backups send prepares)
        if j != self.primary(v) and self.in_wv(v, n):
            self.in_i.add(msg)


    def receive_commit(self, msg):
        (_, v, n, d, j) = msg
        #if j == self.i: return

        if self.view_i >= v and self.in_w(n):
            self.in_i.add(msg)


    def receive_checkpoint(self, msg):
        (_, v, n, d, j) = msg
        if j == self.i: return

        if self.view_i >= v and self.in_w(n):
            self.in_i.add(msg)


    def receive_view_change(self, msg):
        (_, v, n, s, C, P, j) = msg
        if j == self.i: return

        ret = self.correct_view_change(msg, v, j)
        if v >= self.view_i and ret:
            self.in_i.add(msg)


    def receive_new_view(self, msg):
        (_, v, X, O, N, j) = msg
        if j == self.i: return False

        
        cond = v >= self.view_i and v > 0

        senders = set()
        for x in X:
            snd = x[-1]
            cond &= self.correct_view_change(x, v, snd)
            senders.add(snd)

        cond &= len(senders) >= 2 * self.f + 1 
        O2, N2, maxV, maxO, used_ns = self.compute_new_view_sets(v, X)
        cond &= N == N2
        cond &= O == O2
        cond &= not self.has_new_view(v)
        

        if cond:

            P = set()
            for msgx in self.filter_type(self._PREPREPARE, O | N):
                (_, vi, ni, mi, _) = msgx
                P.add( (self._PREPARE, v, ni, self.hash(mi), self.i) )

            self.view_i = v
            self.in_i |= (O | N | P)
            self.in_i.add(msg)
            self.out_i |= P
            return True
        else:
            return False


    # Internal transactions

    def send_preprepare(self, m):
        #print("-----reached send_prepare")
        v = self.view_i #TODO will need to change when make view changes
        n = self.seqno_i + 1
        cond = (self.primary() == self.i)
        #cond &= (n == self.seqno_i + 1) #remove param n
        #print("-----cond: ", cond)
        cond &= self.in_wv(v, n)
        #print("-----cond: ", cond)
        #cond &= self.has_new_view(v) #remove param v
        cond &= m in self.in_i
        #print("-----cond: ", cond)

        # Ensure we only process once.
        cond &= m[0] == self._REQUEST
        #print("-----cond: ", cond)
        for ms in self.filter_type(self._PREPREPARE):
            (_, vp, np, mp, ip) = ms
            cond &= ((vp, mp) != (v, m))
        #print("-----cond: ", cond)
        if cond:
            self.seqno_i = self.seqno_i + 1
            p = (self._PREPREPARE, v, n, m, self.i)
            self.out_i.add(p)
            self.in_i.add(p)

            return PrePrepare(v, n, m, self.i)
        else:
            return None


    def send_commit(self, m, v, n):
        c = (self._COMMIT, v, n, self.hash(m), self.i)
        if c not in self.in_i and self.prepared(m,v,n):
            self.out_i.add(c)
            self.in_i.add(c)
            return True
        else:
            return False


    def execute(self, m, v, n):
        #return self.current_value
        #print("----execute: last_exec_i: ", self.last_exec_i)
        #print("----execute: n: ", n)

        if n > self.last_exec_i and self.committed(m, v, n):
            self.last_exec_i = n
            if m != None: # TODO: check null representation
                (_, o, t, c) = m
                if True:
                    self.last_rep_ti[c] = t
                    self.last_rep_i[c], self.vali = None, None # TODO: EXEC
                    self.current_value = o
                    self.last_rep_i[c] = o
                #if datetime.time(t) >= self.last_rep_ti[c]: #TODO get timestamps working
                    #if t > self.last_rep_ti[c]:
                        # self.last_rep_ti[c] = t
                        # self.last_rep_i[c], self.vali = None, None # TODO: EXEC
                        # self.current_value = o
                        # self.last_rep_i[c] = o
        #                 #if self.i == 1:
        #                 #    print("********** %s:%s" % (self.last_exec_i, (t, c)) )
                    ret = (self._REPLY, self.view_i, t, c, self.i, self.last_rep_i[c])
                    self.out_i.add(ret)
            self.in_i.discard(m)
            return ret

        #     if self.take_chkpt(n):
        #         new_chkpt = self.to_checkpoint(self.vali, self.last_rep_i, self.last_rep_ti)
        #         m = (self._CHECKPOINT, self.view_i, n, new_chkpt, self.i)
        #         self.in_i.add(m)
        #         self.out_i.add(m)
        #         self.checkpts_i.add((n, new_chkpt))

        #     return True
        else:
            return None

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

    # transaction must be in form "snder-receiver-val", where snder and receiver is A, B, or C and val is an integer value
    def update_ledger(self, transaction, isLeaf):
        if isLeaf:
            #update ledger and accounts
            try:
                sndr, rcvr, amount = transaction.split('-', 2)
                amount = int(amount)
                self.ledger.append(transaction)
                self.transactions.append(transaction)
                self.accounts[sndr] -= amount
                self.accounts[rcvr] += amount
                print("------update_ledger: new ledger: ", self.ledger)
                return (sndr + ":$" + str(self.accounts[sndr]) + '--' + rcvr + ":$" + str(self.accounts[rcvr]))     
            except Exception:
                print('Invalid proposal format: ', msg)
                import traceback
                traceback.print_exc()    
        else:
            #update ledger only
            l = transaction.strip('][').replace('\'', '').split(", ")
            #TODO: for each transaction in l, check some things then update or put to temp lsit
            for x in l:
                sndr, rcvr, amount = x.split('-', 2)
                if (self.same_ledger(sndr, rcvr)):
                    #intra cluster transaction
                    self.ledger.append(x)
                else:
                    #cross cluster transaction
                    if sndr in config.descendants[self.network_uid] and rcvr in config.descendants[self.network_uid]:
                        # need to wait for both transactions before appending to ledger
                        if x in self.pending_inter_ledger:
                            self.pending_inter_ledger.remove(x)
                            self.ledger.append(x)
                        else:
                            self.pending_inter_ledger.append(x)
                    else:
                        #print("------------ Update ledger: REACHED")
                        self.ledger.append(x)


            print("------update_ledger: new ledger: ", self.ledger)
        


    def compute_P(self, v, M=None):
        if M is None:
            M = self.in_i

        by_ni = {}
        for prep in self.filter_type(self._PREPREPARE, M):
            (_, vi,ni, mi, _) = prep

            if self.prepared(mi, vi, ni, M):
                if ni not in by_ni:
                    by_ni[ni] = prep
                else:
                    if by_ni[1] < vi:
                        by_ni[n1] = prep

        P = set()
        for prep in by_ni.values():
            P.add(prep)
            (_, vi2,ni2, mi2, _) = prep

            for mx in self.filter_type(self._PREPARE, self.in_i): 
                if mx[:4] == (self._PREPARE, vi2, ni2, self.hash(mi2)):
                    if mx[4] != self.primary(vi2):
                        P.add(mx)

        return frozenset(P)

    def compute_C(self, n=None, s=None, M=None):
        if M is None:
            M = self.in_i

        if n is None or s is None:
            n, s = self.stable_n(), self.stable_chkpt(),

        C = set()
        for m in self.filter_type(self._CHECKPOINT, M):
            (_, v, np, dp, j) = m
            if np == n and s == dp:
                C.add(m)
        return frozenset(C)



    def send_viewchange(self, v):
        if v == self.view_i + 1:
            self.view_i = v

            P = self.compute_P(v)
            C = self.compute_C()

            sn, shkpt = self.stable_n(), self.stable_chkpt(),
            msg = (self._VIEWCHANGE, v, sn, shkpt, C, P, self.i)
            self.out_i.add(msg)
            self.in_i.add(msg)
            return True
        else:
            return False


    def compute_new_view_sets(self, v, V):
        mergeP = set()
        maxV = 0
        for (_, _, n, s, C, P, _) in V:
            mergeP |= P
            maxV = max(maxV, n)

        # The set O contains fresh preprepares
        O = set()
        used_ns = set()
        for msg in self.filter_type(self._PREPREPARE, mergeP):
            (_, vi,ni, mi, _) = msg
            if ni > maxV:
                new_prep = (self._PREPREPARE, v, ni, mi, self.primary(v))
                O.add(new_prep)
                used_ns.add(ni)
        O = frozenset(O)

        # The set N contrains nulls for the non-proposed slots
        N = set()

        maxO = 0
        if len(used_ns) > 0:
            maxO =max(used_ns)

        for ni in range(maxV+1, maxO+1):
            if ni not in used_ns:
                new_prep = (self._PREPREPARE, v, ni, None, self.primary(v))
                N.add(new_prep)
        N = frozenset(N)

        return O, N, maxV, maxO, used_ns


    def send_newview(self, v, V):
        cond = (self.primary(v) == self.i)
        cond &= (v >= self.view_i and v > 0)
        for Vi in V:
            cond &= (Vi in self.in_i)
        cond &= len(V) == 2 * self.f + 1
        cond &= not self.has_new_view(v)
        
        who = set()
        same = None
        for Vi in V:
            (xtype, xv, xn, xs, xC, xP, peer_k) = Vi
            cond &= (xtype, xv) == (self._VIEWCHANGE, v)
            cond &= same == None or (xn, xs, xC, xP) == same
            same = (xn, xs, xC, xP)
            who.add(peer_k)
        
        cond &= (len(who) >= (2 * self.f + 1))

        if cond:
            (O, N, maxV, maxO, used_ns) = self.compute_new_view_sets(v, V)

            m = (self._NEWVIEW, v, frozenset(V), O, N, self.i)
            self.seqno_i = maxO if maxO > 0 else self.seqno_i
            self.in_i.add(m)
            self.in_i |= O
            self.in_i |= N
            self.out_i.add(m) # TODO clear out_i

            self.update_state_nv(v, V, m, maxV)

            for req in list(self.filter_type(self._REQUEST)):
                (_, o, t, c) = req
                if c in self.last_rep_ti and t <= self.last_rep_ti[c]:
                    self.in_i.remove(req)

            return True
        else:
            return False

    def update_state_nv(self, v, V, m, maxV):
        if maxV > self.stable_n():
            (_, _, xn, xs, C, _, _) = V[0]
            if xn == maxV:
                self.in_i |= C

            own_chkpt = (self._CHECKPOINT, v, xn, xs, i)
            if own_chkpt not in self.in_i:
                self.in_i.add(own_chkpt)
                self.out_i.add(own_chkpt)

            for chk in list(self.checkpts_i):
                ni, si = chk
                if ni < maxV:
                    self.checkpts_i.remove(chk)

            if maxV > self.last_exec_i:
                self.checkpts_i.add( (maxV, s) )

            vx = self.stable_chkpt
            self.vali, self.last_rep_i, self.last_rep_ti = from_checkpoint(vx)
            self.last_exec_i = maxV


    def garbage_collect(self):
        counter = defaultdict(set)
        X = 0
        for msg in self.filter_type(self._CHECKPOINT):
            (_, cv, cn, cs, ci) = msg
            counter[(cn, cs)].add(ci)
            X+=1

        n, s = (-1, 0)
        for (cn, cs) in counter:
            # NOTE: Additional check not in spec: (cn, cs) in self.checkpts_i
            if len(counter[(cn, cs)]) > self.f and (cn, cs) in self.checkpts_i:
                n, s = max((n,s), (cn, cs))

        ## TODO: Check this is correct -- not in the spec
        # self.checkpts_i.add( (n, s) )

        # Massive clean-up
        to_delete = set()
        for msg in self.in_i:
            xtype = msg[0]
            if xtype in [self._PREPREPARE, self._PREPARE, \
                         self._COMMIT, self._CHECKPOINT]:
                xn = msg[2]
                if xn < n:
                    to_delete.add(msg)

        self.in_i -= to_delete

        # Now delete the checkpoints
        to_delete_chk = set()
        for (xn, s) in self.checkpts_i:
            if xn < n:
                to_delete_chk.add( (xn, s) )
        self.checkpts_i -= to_delete_chk

        # Now grabage collect requests
        to_delete_req = set()
        for msg in self.filter_type(self._REQUEST):
            (_, o, t, c) = msg
            if c in self.last_rep_ti and self.last_rep_ti[c] == t:
                to_delete_req.add(msg)
    
        self.in_i -= to_delete_req        

        # if n > 0 and len(to_delete):
        #    print("DELETED (%s): %s,%s (in_i=%s stable n=%s)" % (n, len(to_delete), len(to_delete_chk), len(self.in_i), self.stable_n()))
        #    print(Counter([m[0] for m in self.in_i]))



    # System's calls

    def route_receive(self, msg):

        xtype = msg[0]
        self.stat.update([xtype])
        xlen = len(msg)
        if xtype == self._REQUEST and xlen == 4:
            self.receive_request(msg)
            ret = self.send_preprepare(msg, self.view_i, self.seqno_i+1)

            # TODO CHECK CORRECTNESS -- NOT IN SPEC:
            # Check if we are done with this. Then respond again to all:
            (_, o, t, c) = msg
            if c in self.last_rep_ti and self.last_rep_ti[c] == t:
                self.out_i |= set(self.filter_type(self._COMMIT))
                self.out_i |= set(self.filter_type(self._CHECKPOINT))

            
        elif xtype == self._PREPREPARE and xlen == 5:
            self.receive_preprepare(msg)

        elif xtype == self._PREPARE and xlen == 5:
            self.receive_prepare(msg)
            
        elif xtype == self._COMMIT and xlen == 5:
            self.receive_commit(msg)

        elif xtype == self._CHECKPOINT and xlen == 5:
            self.receive_checkpoint(msg)

        elif xtype == self._VIEWCHANGE and xlen == 4 + 3:
            self.receive_view_change(msg)

            # Gather related view changes
            V = set()
            for vc_msg in self.filter_type(self._VIEWCHANGE):
                if vc_msg[1] == msg[1]:
                    V.add(vc_msg) 
            ret = self.send_newview(msg[1], V)
            if ret:
                # Process hanging requests
                for xmsg in list(self.filter_type(self._REQUEST)):
                    self.route_receive(xmsg)


        elif xtype == self._NEWVIEW and xlen == 6:
            self.receive_new_view(msg)
            ret = self.has_new_view(self.view_i)
            
            if ret:
                # Process again any 'hanging' requests
                for xmsg in self.filter_type(self._REQUEST):
                    self.route_receive(xmsg)

        else:
            raise Exception("UNKNOWN type: ", msg)

        # Make as much progress as possible
        all_preps = []
        for prep in self.filter_type(self._PREPREPARE):
            if not (prep[1] >= self.view_i and prep[2] >= self.last_exec_i + 1): continue
            all_preps += [ prep ]

        all_preps = sorted(all_preps, key=lambda xmsg: xmsg[2])

        for (_, vx, nx, mx, _) in all_preps:
                self.send_commit(mx,vx,nx)
                self.execute(mx,vx,nx)

        # Garbage collect
        self.garbage_collect()


    def unhandled_requests(self):
        unhand = list(self.filter_type(self._REQUEST))
        return unhand
            
    def _debug_status(self, request):
        # First check out if the request has been received:
        print("\nPeer %s (view: %s) REQ: %s" % (self.i, self.view_i, str(request)))
        accounted = set()
        for msg in self.in_i:
            if msg[0] == self._PREPREPARE and msg[3] == request:
                accounted.add( (msg[1], msg[2]) )
                print("** %s" % (str(msg)))
                if self.prepared(request, msg[1], msg[2]):
                    print("        ** PREPARED **")

                # How many prepeared do we have
                for Pmsg in self.in_i:
                    if Pmsg[0] == self._PREPARE and Pmsg[1:3] == msg[1:3]:
                        print("        %s" % str(Pmsg))

                committed = False
                if self.committed(request, msg[1], msg[2]):
                    print("        ** COMMITED **")
                    committed = True

                # How many prepeared do we have
                for Pmsg in self.in_i:
                    if Pmsg[0] == self._COMMIT and Pmsg[1:3] == msg[1:3]:
                        print("        %s" % str(Pmsg))

                if committed:
                    if self.last_exec_i >= msg[2]:
                        print("        ** EXECUTED (%s) **" % self.last_exec_i)
                    else:
                        print("        ** NOT EXECUTED (%s) **" % self.last_exec_i)
            
        # How many prepeared do we have
        for Pmsg in self.in_i:
            if Pmsg[0] == self._PREPARE and Pmsg[3] == self.hash(request):
                if Pmsg[1:3] not in accounted:
                    print("STRAY: %s" % str(Pmsg))


class PbftInstance (Replica):
    
    def __init__(self, i, R):
        Replica.__init__(self, i, R)

    # def receive_prepare(self, msg):
    #     self.observe_proposal( msg.proposal_id )
    #     return super(PaxosInstance,self).receive_prepare(msg)
                    
    # def receive_accept(self, msg):
    #     self.observe_proposal( msg.proposal_id )
    #     return super(PaxosInstance,self).receive_accept(msg)
