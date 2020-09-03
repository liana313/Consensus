import sys
import os.path
import argparse
import json

from twisted.internet import reactor

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.dirname(this_dir) )

import config

from replicated_value    import BaseReplicatedValue
from messenger           import Messenger
from sync_strategy       import SimpleSynchronizationStrategyMixin
from resolution_strategy import ExponentialBackoffResolutionStrategyMixin
#from master_strategy     import DedicatedMasterStrategyMixin


p = argparse.ArgumentParser(description='Multi-Paxos replicated value server')
p.add_argument('uid', choices=['1000', '1001', '1002', '2000', '2001', '2002', '3000', '3001', '3002', '4000', '4001', '4002', '100', '101', '102', '200', '201', '202', '300', '301', '302', '400', '401', '402', '10', '11', '12', '20', '21', '22', '1', '2', '3'], help='UID of the server. Must be 1000, 1001, or 1002')
p.add_argument('--master', action='store_true', help='If specified, a dedicated master will be used. If one server specifies this flag, all must')

args = p.parse_args()


if args.master:

    class ReplicatedValue(DedicatedMasterStrategyMixin, ExponentialBackoffResolutionStrategyMixin, SimpleSynchronizationStrategyMixin, BaseReplicatedValue):
        '''
        Mixes the dedicated master, resolution, and synchronization strategies into the base class
        '''
else:
    
    class ReplicatedValue(ExponentialBackoffResolutionStrategyMixin, BaseReplicatedValue):
    	#class ReplicatedValue(BaseReplicatedValue):
        '''
        Mixes just the resolution and synchronization strategies into the base class
        '''

#TODO implment state file use for crash recovery
state_file = config.state_files['1000']

# get height and cluster that server is in
height = 4 - len(args.uid)
print("height: ", height)
cluster = 0
if int(args.uid) == 1 or int(args.uid) == 2 or int(args.uid) == 3:
	cluster = 3
else:
	cluster = int(args.uid[0])

print("cluster: ", cluster)
print("peers: ", config.peers[height][cluster].keys())

#r = ReplicatedValue(args.uid, config.peers.keys(), state_file)
r = ReplicatedValue(args.uid, config.peers[height][cluster].keys(), state_file)
#m = Messenger(args.uid, config.peers, r)
m = Messenger(args.uid, config.peers[height][cluster], r)

reactor.run()

