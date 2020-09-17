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
#from sync_strategy       import SimpleSynchronizationStrategyMixin
from resolution_strategy import ExponentialBackoffResolutionStrategyMixin
#from master_strategy     import DedicatedMasterStrategyMixin


p = argparse.ArgumentParser(description='PBFT replicated value server')
p.add_argument('uid', choices=['1000', '1001', '1002', '1003', '2000', '2001', '2002', '2003', '3000', '3001', '3002', '3003', '4000', '4001', '4002', '4003', '100', '101', '102', '103', '200', '201', '202', '203', '300', '301', '302', '303','400', '401', '402', '403', '10', '11', '12', '13', '20', '21', '22', '23','1', '2', '3', '4'], help='UID of the server. Must be valid num - see config')
p.add_argument('--master', action='store_true', help='If specified, a dedicated master will be used. If one server specifies this flag, all must')

args = p.parse_args()

#TODO implement state file for crasy recovery
state_file = config.state_files['1000']
# get height and cluster that server is in
height = 4 - len(args.uid)
print("height: ", height)
cluster = 0
if int(args.uid) == 1 or int(args.uid) == 2 or int(args.uid) == 3 or int(args.uid) == 4:
	cluster = 1
else:
	cluster = int(args.uid[0])

print("cluster: ", cluster)
print("peers: ", config.peers[height][cluster].keys())

if args.master:

    class ReplicatedValue(DedicatedMasterStrategyMixin, ExponentialBackoffResolutionStrategyMixin, SimpleSynchronizationStrategyMixin, BaseReplicatedValue):
        '''
        Mixes the dedicated master, resolution, and synchronization strategies into the base class
        '''
else:
    
    class ReplicatedValue(BaseReplicatedValue):
    	#class ReplicatedValue(BaseReplicatedValue):
        '''
        Mixes just the resolution and synchronization strategies into the base class
        '''





r = ReplicatedValue(args.uid, config.peers[height][cluster].keys(), state_file)
m = Messenger(args.uid, config.peers[height][cluster], r)


reactor.run()

