#!/usr/bin/python

#
# Script for testing neo, zeo and file storage perfomances
#
from NEO import neostorage
import ZODB
from Persistence import Persistent
from ZODB.FileStorage import FileStorage
from ZODB.tests.StorageTestBase import removefs
from ZODB.utils import oid_repr, p64, u64, U64
from ZEO.ClientStorage import ClientStorage
from ZEO.tests import forker
from timeit import Timer
from random import choice, randrange
import sys, os, signal

class foo(Persistent):
  """ Object use in transaction, must implement
  some variable and funtion to work correctly """
  
  def __init__(self, jar = None, oid = None):
    self._p_jar = jar
    self._p_oid = oid
    self._p_changed = 1
  
  def __getstate__(self):
    pass

listOid = []
listTrans = []

def StoreTest(c):
  """ Store many objects in one transaction """
  try:
    # init
    t = ZODB.Transaction.Transaction()
    c.tpc_begin(t)
    # create objects
    for i in xrange(100000):
      oid = c.new_oid()
      listOid.append(oid)
      obj = foo(c, oid)
      obj.test = U64(oid)
      # store it
      c.commit(obj, t)
    # finish transaction
    c.tpc_vote(t)
    c.tpc_finish(t)
  except:
    raise


def LoadTest(c):
  """ Load many objects """
  try:
    for oid in listOid:
#      c[oid]
      c._storage.load (oid, None)
  except:
    raise


def GetHistoryTest(db):
  """ Get history foe each object """
  for oid in listOid:
    db.history(oid,None)


def LoadFromSerialTest(c):
  """ Load serial version of object """
  for oid in listOid:
    s = c._storage.getSerial(oid)
    c._storage.loadSerial(oid, s)


def MakeTransactionTest(c):
  """ Make many transactions """
  try:
    for i in xrange(5000):
      t = ZODB.Transaction.Transaction()
#       listTrans.append(t)
      c.tpc_begin(t)
      # create one object per transaction
      oid = c.new_oid()
      obj = foo(c, oid)
      obj.test = '25'
      c.commit(obj, t)
      # finish transaction
      c.tpc_vote(t)
      c.tpc_finish(t)
  except:
    raise


def UndoTransactionTest(c, db):
  """ Undo some transactions """
  for i in xrange(10):
    log = db.undoLog(0,-50)
    tid = choice(log)['id']
    db.undo(tid)

def getSizeTest(db):
  """ Get size of storage """
  db.getSize()

def new_oidTest(c):
  """ Get many new oid """
  for i in range(100000):
    oid = c.new_oid()
    # print len(oid)



# class to test performances
class perf:
  """ super class for performances test
  a subclass might override """
  
  def __init__(self):
    self.storage = None
    
  def run(self):
    self.db = ZODB.DB(self.storage)
    self.c = self.db.open()
    # tests
    t = Timer("MakeTransactionTest(perf.c)", "from __main__ import MakeTransactionTest, perf")
    print 'MakeTransaction test time : %.5f s'  % t.timeit(number=1)
    t = Timer("StoreTest(perf.c)", "from __main__ import StoreTest, perf")
    print 'Store test time : %.5f s'  % t.timeit(number=1)
    t = Timer("LoadTest(perf.c)", "from __main__ import LoadTest, perf")
    print 'Load test time : %.5f s'  % t.timeit(number=1)
#     t = Timer("LoadFromSerialTest(perf.c)", "from __main__ import LoadFromSerialTest, perf")
#     print 'Load from serial test time : %.5f s'  % t.timeit(number=1)
#     t = Timer("GetHistoryTest(perf.db)", "from __main__ import GetHistoryTest, perf")
#     print 'Get history test time : %.5f s'  % t.timeit(number=1)
#     t = Timer("new_oidTest(perf.c)", "from __main__ import new_oidTest, perf")
#     print 'New oid test time : %.5f s'  % t.timeit(number=1)
    # XXX not use for test yet
    #   t = Timer("UndoTransactionTest(c, db)", "from __main__ import UndoTransactionTest, c, db")
    #   print 'UndoTransaction test time : %.5f s'  % t.timeit(number=1)
    #   t = Timer("getSizeTest(db)", "from __main__ import getSizeTest, db")
    #   print 'getSize test time : %.5f s'  % t.timeit(number=1)  
    


class neo_perf(perf):
  """ class to mesure neo performances """
  def __init__(self):
    self.storage = neostorage.neostorage(master = '127.0.0.1')    

  def __del__(self):
    self.db.close()
    self.c._storage.closeStorageConnection()
    pid = os.getpid()
    os.kill(pid, signal.SIGTERM)    


class zeo_perf(perf):
  """ class to mesure zeo performances """
  def __init__(self):
    # create zeo config
    storage_conf = """<filestorage 1>
    path /home/aurelien/zeo_instance/var/Data.fs
    </filestorage>
    <eventlog>
    level error
    <logfile>
    path /home/aurelien/zeo.log
    </logfile>
    </eventlog>
    """    
    zaddr = '', randrange(20000, 30000)
    zeo_conf = forker.ZEOConfig(zaddr)
    # create zeo server
    t, addr, pid, exitobj = forker.start_zeo_server(storage_conf, zeo_conf, zaddr[1])
    # create db and connection to storage
    self.storage = ClientStorage(zaddr, debug=0, min_disconnect_poll=0.5, wait=1)

  def __del__(self):
    self.db.close()
  

class fs_perf(perf):
  """ class to mesure file storage performances """

  def __init__(self):
    self.storage = FileStorage("cluster.fs")

  def __del__(self):
    self.db.close()
    removefs("cluster.fs")



    
if __name__ == "__main__":

  if  len(sys.argv) !=2:
    sys.exit('argument missing')
  if sys.argv[1] == 'neo':
    perf = neo_perf()
  elif sys.argv[1] == 'fs':
    perf = fs_perf()
  elif sys.argv[1] == 'zeo':
    perf = zeo_perf()
  else:
    sys.exit('wrong argument')

  perf.run()




  
