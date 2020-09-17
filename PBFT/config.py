
# # (IP,UDP Port Number) # A will be leader proposer
# peers = dict( A=('127.0.0.1',10234),
#               B=('127.0.0.1',10235),
#               C=('127.0.0.1',10236) )
# peer_uid = dict(A = 0, B = 1, C = 2)
# client = ('127.0.0.1', 10233)

# # State files for crash recovery. Windows users will need to modify
# # these.
# state_files = dict( A='/tmp/A4.json',
#                     B='/tmp/B4.json',
#                     C='/tmp/C4.json' )




#algorithm type - optimistic or coordinator
#algorithm = 'optimistic'
algorithm = 'coordinator'

#cluster<height><cluster number> - set of uid's in cluster
cluster01 = ["1000", "1001", "1002", "1003"]
cluster02 = ["2000", "2001", "2002", "2003"]
cluster03 = ["3000", "3001", "3002", "3003"]
cluster04 = ["4000", "4001", "4002", "4003"]
# cluster11 = ["100", "101", "102"]
# cluster12 = ["200", "201", "202"]
# cluster13 = ["300", "301", "302"]
# cluster14 = ["400", "401", "402"]
# cluster21 = ["10", "11", "12"]
# cluster22 = ["20", "21", "22"]
cluster31 = ["1", "2", "3", "4"]


#dictionary mapping from node to a list of its leaf level descendants
descendants = dict()
descendants['1'] = descendants['2'] = descendants['3'] = descendants['4'] = cluster01 + cluster02 + cluster03 + cluster04
descendants['10'] = descendants['11'] = descendants['12'] = descendants['13'] = cluster01 + cluster02
descendants['20'] = descendants['21'] = descendants['22'] = descendants['23'] = cluster03 + cluster04
descendants['100'] = descendants['101'] = descendants['102'] = descendants['103'] = cluster01
descendants['200'] = descendants['201'] = descendants['202'] = descendants['203'] = cluster02
descendants['300'] = descendants['301'] = descendants['302'] = descendants['303'] = cluster03
descendants['400'] = descendants['401'] = descendants['402'] = descendants['403'] = cluster04



lca = dict() #sndr's cluster at h=0 -> rcvr's cluster at h=0 -> (addr of lca, id of lca)
lca[(1,2)] = (('127.0.0.1',10258), '10')
lca[(1,3)] = (('127.0.0.1',10264), '1')
lca[(1,4)] = (('127.0.0.1',10264), '1')
lca[(2,1)] = (('127.0.0.1',10258), '10')
lca[(2,3)] = (('127.0.0.1',10264), '1')
lca[(2,4)] = (('127.0.0.1',10264), '1')
lca[(3,1)] = (('127.0.0.1',10264), '1')
lca[(3,2)] = (('127.0.0.1',10264), '1')
lca[(3,4)] = (('127.0.0.1',10261), '20')
lca[(4,1)] = (('127.0.0.1',10264), '1')
lca[(4,2)] = (('127.0.0.1',10264), '1')
lca[(4,3)] = (('127.0.0.1',10261), '20')


peers = dict() # height -> cluster -> id -> (ip, port)
peers = {0: {1: {'1000': -1}, 2: {'2000': -1}, 3: {'3000': -1}, 4: {'4000': -1}}, 1: {1: {'100': -1}, 2: {'200': -1}, 3: {'300': -1}, 4: {'400': -1}}, 2: {1: {'10': -1}, 2: {'20': -1}}, 3: {1: {'1': -1}}}

peers[0][1]['1000'] = ('127.0.0.1',10234)
peers[0][1]['1001'] = ('127.0.0.1',10235)
peers[0][1]['1002'] = ('127.0.0.1',10236)
peers[0][1]['1003'] = ('127.0.0.1',10267)

peers[0][2]['2000'] = ('127.0.0.1',10237)
peers[0][2]['2001'] = ('127.0.0.1',10238)
peers[0][2]['2002'] = ('127.0.0.1',10239)
peers[0][2]['2003'] = ('127.0.0.1',10268)

peers[0][3]['3000'] = ('127.0.0.1',10240)
peers[0][3]['3001'] = ('127.0.0.1',10241)
peers[0][3]['3002'] = ('127.0.0.1',10242)
peers[0][3]['3003'] = ('127.0.0.1',10269)

peers[0][4]['4000'] = ('127.0.0.1',10243)
peers[0][4]['4001'] = ('127.0.0.1',10244)
peers[0][4]['4002'] = ('127.0.0.1',10245)
peers[0][4]['4003'] = ('127.0.0.1',10270)


peers[1][1]['100'] = ('127.0.0.1',10246)
peers[1][1]['101'] = ('127.0.0.1',10247)
peers[1][1]['102'] = ('127.0.0.1',10248)
peers[1][1]['103'] = ('127.0.0.1',10271)

peers[1][2]['200'] = ('127.0.0.1',10249)
peers[1][2]['201'] = ('127.0.0.1',10250)
peers[1][2]['202'] = ('127.0.0.1',10251)
peers[1][2]['203'] = ('127.0.0.1',10272)

peers[1][3]['300'] = ('127.0.0.1',10252)
peers[1][3]['301'] = ('127.0.0.1',10253)
peers[1][3]['302'] = ('127.0.0.1',10254)
peers[1][3]['303'] = ('127.0.0.1',10273)

peers[1][4]['400'] = ('127.0.0.1',10255)
peers[1][4]['401'] = ('127.0.0.1',10256)
peers[1][4]['402'] = ('127.0.0.1',10257)
peers[1][4]['403'] = ('127.0.0.1',10274)

peers[2][1]['10'] = ('127.0.0.1',10258)
peers[2][1]['11'] = ('127.0.0.1',10259)
peers[2][1]['12'] = ('127.0.0.1',10260)
peers[2][1]['13'] = ('127.0.0.1',10275)

peers[2][2]['20'] = ('127.0.0.1',10261)
peers[2][2]['21'] = ('127.0.0.1',10262)
peers[2][2]['22'] = ('127.0.0.1',10263)
peers[2][2]['23'] = ('127.0.0.1',10276)

peers[3][1]['1'] = ('127.0.0.1',10264)
peers[3][1]['2'] = ('127.0.0.1',10265)
peers[3][1]['3'] = ('127.0.0.1',10266)
peers[3][1]['4'] = ('127.0.0.1',10277)


client  = dict()
client['1000'] = ('127.0.0.1', 10230)
client['2000'] = ('127.0.0.1', 10231)
client['3000'] = ('127.0.0.1', 10232)
client['4000'] = ('127.0.0.1', 10233)

server = dict()
server['1000'] = ('127.0.0.1',10234)
server['2000'] = ('127.0.0.1',10237)
server['3000'] = ('127.0.0.1',10240)
server['4000'] = ('127.0.0.1',10243)

#id leader in above level a lead proposer will send updates to - dict node id -> (ip, port) of parent cluster's lead proposer
leader = dict()
leader['1000'] = ('127.0.0.1',10246) #uid 100
leader['2000'] = ('127.0.0.1',10249) #uid 200
leader['3000'] = ('127.0.0.1',10252) #uid 300
leader['4000'] = ('127.0.0.1',10255) #uid 400
leader['100'] = ('127.0.0.1',10258) #uid 10
leader['200'] = ('127.0.0.1',10258) #uid 10
leader['300'] = ('127.0.0.1',10261) #uid 20
leader['400'] = ('127.0.0.1',10261) #uid 20
leader['10'] = ('127.0.0.1',10264) #uid 1
leader['20'] = ('127.0.0.1',10264) #uid 1

# State files for crash recovery. Windows users will need to modify
state_files = dict()
state_files['1000'] = '/tmp/1000.json'
state_files['1001'] = '/tmp/1001.json'
state_files['1002'] = '/tmp/1002.json'
