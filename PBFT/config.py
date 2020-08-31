
# (IP,UDP Port Number) # A will be leader proposer
peers = dict( A=('127.0.0.1',10234),
              B=('127.0.0.1',10235),
              C=('127.0.0.1',10236) )
peer_uid = dict(A = 0, B = 1, C = 2)
client = ('127.0.0.1', 10233)

# State files for crash recovery. Windows users will need to modify
# these.
state_files = dict( A='/tmp/A4.json',
                    B='/tmp/B4.json',
                    C='/tmp/C4.json' )
