#! /usr/bin/env python

import unittest, os, signal, struct, socket, time
from ZODB import Transaction
from ZODB.POSException import UndoError, POSKeyError, MultipleUndoErrors, StorageTransactionError
from ZODB.utils import oid_repr, p64, u64, U64
from NEO import neostorage

login = "aurelien"
config_file  = "neo.conf"

storages = {}
master_ip = None

def createTransaction():
  txn = Transaction.Transaction()
  txn.user = "unit tester"
  txn.description = "transaction for unit tests"
  txn.extension = "None"
  return txn


def setupNodes():
  """ instore the storage and master node from the neo.conf file """
  f = open (config_file, "r")
  line = f.readline()
  while line != '':
    if line[0] == '#': # comment line
      line = f.readline()  
      continue    
    line = line.split(' ', 3)
    type = line[0]
    ip = line[1]
    path = line[2]
    options = line[3][:-1]
    if type == 'master':
      global master_ip
      master_ip = ip
      os.system('ssh '+login+'@'+ip+' '+path+'neomaster '+options+' &>/dev/null &') # &>/dev/null
    elif type == 'storage':
      global storages
      storages[len(storages)] = (ip, path)
      os.system('ssh '+login+'@'+ip+' '+path+'neostorage '+options+' -m'+master_ip+' &')
    time.sleep (2)
    line = f.readline()  

class foo:
  pass


class NeoTests (unittest.TestCase):
  """ Unit test class """
  def setUp (self):
    global master_ip
    time.sleep (2)
    self.storage = neostorage.neostorage(master = master_ip)

  def tearDown (self):
    self.storage.close()

  def testLoadGetObject (self):
    """ store and load the same object, then compare data """
    t = createTransaction()
    tid = self.storage.tpc_begin(t)
    oid = self.storage.new_oid()
    obj1 = foo()
    obj1.data = 'data for unit test'
    self.storage.store(oid, tid, obj1, 0, t)
    self.storage.tpc_vote(t)
    self.storage.tpc_finish(t)
    obj2, s2 = self.storage.load(oid, 0)
    self.assertEqual (obj1.data, obj2.data)

  def testLoadUnknownObject (self):
    """ try to load an unknown object """
    oid = self.storage.new_oid()
    self.assertRaises(POSKeyError, self.storage.load, oid, 0)

  def testGetHistory (self):
    """ check if history for an object stord is not None """
    t = createTransaction()
    tid = self.storage.tpc_begin(t)
    oid = self.storage.new_oid()
    obj1 = foo()
    obj1.data = 'data testing'
    self.storage.store(oid, tid, obj1, 0, t)
    self.storage.tpc_vote(t)
    self.storage.tpc_finish(t)
    hist = self.storage.history(oid, 1)
    self.assertEqual (hist[0]['user_name'], 'unit tester')
    self.assertEqual (U64(hist[0]['serial']), U64(tid))
    
  def testIncreasingOid (self):
    oid_list = []
    for i in xrange (20):
      oid_list.append(self.storage.new_oid())
    for i in xrange (19):
      self.failIf (oid_list[i] > oid_list[i+1])
      
  def testTwoTransactions (self):
    """ Transaction can be done just one at a time """
    t1 = createTransaction()
    t2 = createTransaction()
    self.storage.tpc_begin(t1)
    self.assertRaises (StorageTransactionError, self.storage.tpc_vote, t2)

  def testUndoTransaction (self):
    """ check if object from undone transaction doesn't exist """
    t = createTransaction()
    tid = self.storage.tpc_begin(t)
    oid = self.storage.new_oid()
    obj1 = foo()
    obj1.data = 'data testing'
    self.storage.store(oid, tid, obj1, 0, t)
    self.storage.tpc_vote(t)
    self.storage.transactionalUndo(tid, t)
    self.assertRaises (POSKeyError, self.storage.loadSerial, oid, tid)

  def testUndoUnknownTransaction(self):
    t = createTransaction
    self.storage.tpc_begin(t)
    tid = self.storage.new_oid()
    self.assertRaises (UndoError , self.storage.transactionalUndo, tid, t)

  def testUndoInfo (self):
    t = createTransaction()
    tid = self.storage.tpc_begin(t)
    oid = self.storage.new_oid()
    obj1 = foo()
    obj1.data = 'data testing'
    NO_VERSION = 0
    self.storage.store(oid, tid, obj1, 0, t)
    self.storage.tpc_vote(t)
    self.storage.tpc_finish(t)
    undo = self.storage.undoInfo()
    self.assertEqual(undo[0]['username'], 'unit tester')
    self.assertEqual(U64(undo[0]['id']), U64(tid))

  def testSupportVersion (self):
    self.assertEqual(0, self.storage.supportsVersions())

  def testSupportUndo (self):
    self.assertEqual(1, self.storage.supportsUndo())

  def testStorageName (self):
    self.assertEqual('Cluster Storage system', self.storage.getName())

  def testStorageName (self):
    self.failIf(self.storage.getSize() < 0)
    
  def testLoadSerial (self):
    # make a first transaction
    t = createTransaction()
    tid1 = self.storage.tpc_begin(t)
    oid = self.storage.new_oid()
    obj1 = foo()
    obj1.data = 'data testing'
    self.storage.store(oid, None, obj1, 0, t)
    self.storage.tpc_vote(t)
    self.storage.tpc_finish(t)
    # make a second one with the same object
    t = createTransaction()
    tid2 = self.storage.tpc_begin(t)
    obj2 = foo()
    obj2.data = 'updated data testing'
    self.storage.store(oid, tid1, obj2, 0, t)
    self.storage.tpc_vote(t)
    self.storage.tpc_finish(t)
    # load old one
    obj3 = self.storage.loadSerial(oid, tid1)
    self.assertEqual(obj1.data, obj3.data)
    # get serial
    self.assertEqual(tid2, self.storage.getSerial(oid))
    # load last one
    obj4, s4 = self.storage.load(oid, 0)
    self.assertEqual(obj2.data, obj4.data)


#   def testMasterBadMessages (self):
#     """ test if master node doesn't crash with bad format message """
#     for i in xrange (26):
#       buf = struct.pack('!HccI 40s', i, ' ', ' ', 40000, 'p'*40000 )
#       self.storage.MasterSock.sendall(buf)
#     # dont call method 26 otherwise it close connection
#     for i in xrange (27, 50):
#       buf = struct.pack('!HccI 40s', i, ' ', ' ', 40000, 'p'*40000 )
#       self.storage.MasterSock.sendall(buf)
      

#   def testStorageBadMessages (self):
#     """ test if storage node doesn't crash with bad format message """
#     sock = None
#     # get one storage
#     s = neostorage.storages.popitem()
#     for i in xrange(28):
#       buf = struct.pack('!HccI 40s', i, ' ', ' ', 40000, 'p'*40000 )
#       sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#       sock.connect((s[1][0], s[1][1]))
#       sock.sendall(buf)
#       sock.close()
#     # don't call method 28 otherwise storage close
#     for i in xrange(29,50):
#       buf = struct.pack('!HccI 40s', i, ' ', ' ', 40000, 'p'*40000 )
#       sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#       sock.connect((s[1][0], s[1][1]))
#       sock.sendall(buf)
#       sock.close()

#   def testBigSizeMessage (self):
#     """ test a big message """
#     buf = struct.pack('!HccLs', 10, ' ', ' ', 929496729, 'p'*929496729 )
#     self.storage.MasterSock.sendall(buf)
      

def test_suite():
  s = unittest.makeSuite(NeoTests, 'test')
  return s

def kill_neo():
  os.system('ssh '+login+'@'+master_ip+' killall neomaster')
  for storage in storages.values():
    os.system('ssh '+login+'@'+str(storage[0])+' killall neostorage')

    
if __name__=='__main__':

  setupNodes()
  unittest.TextTestRunner().run(test_suite())
  # kill unit test program
  kill_neo()
  pid = os.getpid()
  os.kill(pid, signal.SIGTERM)

