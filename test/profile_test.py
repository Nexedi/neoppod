#!/usr/bin/python

#
# Script to profile neo and zeo storage system
#
from NEO import neostorage
from Persistence import Persistent
import ZODB, profile, pstats, sys
from random import randrange
from ZEO.ClientStorage import ClientStorage
from ZEO.tests import forker

listOid = []


class foo(Persistent):
  """ Test object """
  
  def __init__(self, jar = None, oid = None):
    self._p_jar = jar
    self._p_oid = oid
    self._p_changed = 1
  
  def __getstate__(self):
    pass


def MakeTransactionTest(c):
  """ Make many transaction """
  try:
    for i in xrange(1000):
      t = ZODB.Transaction.Transaction()

      c.tpc_begin(t)
      # create one object
      oid = c.new_oid()
      obj = foo(c, oid)
      obj.test = '25'
      c.commit(obj, t)
      # finish transaction
      c.tpc_vote(t)
      c.tpc_finish(t)      
  except:
    raise


def StoreTest(c):
  """ Store many object in one transaction """
  try:
    # init
    t = ZODB.Transaction.Transaction()
    c.tpc_begin(t)
    # create objects
    for i in xrange(5000):
      oid = c.new_oid()
      listOid.append(oid)
      obj = foo(c, oid)
      obj.test = oid
      # store it
      c.commit(obj, t)
    # finish transaction
    c.tpc_vote(t)
    c.tpc_finish(t)
  except:
    raise

def LoadTest(c):
  """ Load many object from the storage """
  for oid in listOid:
    c._storage.load (oid, None)

def GetHistoryTest(c, db):
  """ Get history from object """
  for oid in listOid:
    db.history(oid,None)


def LoadFromSerialTest(c):
  """ Load some Serial """
  for oid in listOid:
    s = c._storage.getSerial(oid)
    c._storage.loadSerial(oid, s)



class profiler:
  """ super class for profiling storage system
  a dubclass might override """

  def __init__(self):
    self.storage = None

  def run(self):
    db = ZODB.DB(self.storage)
    c = db.open()
    # make test
    MakeTransactionTest(c)
    StoreTest(c)
    LoadTest(c)
    LoadFromSerialTest(c)
    GetHistoryTest(c, db)


class neo_profiler(profiler):
  """ class to profile neo application execution """

  def __init__(self):
    self.storage = neostorage.neostorage(master = '127.0.0.1')
    self.run()


class zeo_profiler(profiler):
  """ class to profile zeo application execution """

  def __init__(self):
    storage_conf = """<filestorage 1>path /home/aurelien/zeo_instance/var/Data.fs</filestorage> """    
    zaddr = '', randrange(20000, 30000)
    zeo_conf = forker.ZEOConfig(zaddr)
    # create zeo server
    t, addr, pid, exitobj = forker.start_zeo_server(storage_conf, zeo_conf, zaddr[1])
    # create db and connection to storage
    self.storage = ClientStorage(zaddr, debug=0, min_disconnect_poll=0.5, wait=1)
    self.run()



if __name__ == "__main__":

  if  len(sys.argv) !=2:
    sys.exit('argument missing')
  if sys.argv[1] == 'neo':  
    profile.run ('neo_profiler()', 'result')
  elif sys.argv[1] == 'zeo':
    profile.run ('zeo_profiler()', 'result')
  else:
    sys.exit ('wrong argument')

  # display result
  p = pstats.Stats('result')
  p.strip_dirs().sort_stats('cumulative').print_stats(30)
  p.print_callees()
