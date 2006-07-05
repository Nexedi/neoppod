from mq import MQ
from ZODB import POSException, Transaction
from ZODB.utils import oid_repr, p64, u64, U64
from threading import Thread, Lock, Condition
from cPickle import dumps, loads
from zlib import crc32
from time import gmtime, localtime
import neoclient
import socket
import signal
import random
import gc
import syslog
import sys
import os, time
from ZODB.BaseStorage import BaseStorage
from ZODB.TimeStamp import TimeStamp
from DateTime import DateTime

import traceback

MASTER_PORT = 10823
CLIENT_PORT = 11111
MAX_STORAGE_CONNECTION = 5
debug = 0


# Client cache using mutli-queue algorithm
# the key is the oid, and the value is a tuple
# containing the serial number and object's data
cache = MQ()
cache_lock = Lock()
# storage dict : id -> (ipv4addr, port); and client dict (idem)
storages = {}
clients = {}
# storage connection list : storage id -> file descriptor for connection
storages_connection = {}
# XXX master connection must be global too...
id = None

class neostorage_server_thread(Thread):
  """ class handle each client connection from other client """
  def __init__(self, socket, **kw):
    Thread.__init__(self, **kw)
    self.socket = socket
    self.cache_lock = cache_lock
    self.methods = {19:self.checkCache,
                    13:self.addSN,
                    14:self.addCN,                   
                    15:self.delSN,
                    16:self.delCN,
                    28:self.masterClose,
                    32:self.masterChange,
                    33:self.unreliableStorage,  
                    }
    if debug : print '*** New Client Server thread running ***'


  def run(self):  
    while 1: # connection layer return a tuple, first item is method id
      data = neoclient.wait(self.socket.fileno())
      if debug : print 'neoclient : data received with key %d' %data[0]
      # waitMsg
      if self.methods.has_key(data[0]):
        self.methods[data[0]](data[1:])
      self.socket.close()
      return
        

  def checkCache(self, ob):
    """ invalidate list of objects send by other client """
    print 'check cache for oid %r' %(ob,)
    for oid in ob:
      self.cache_lock.acquire()
      try:
        if p64(long(oid)) in cache:
          try:
            del cache[oid]
          except:
            pass
      finally:
        self.cache_lock.release()
    print 'check done'

  def addSN(self,sn):
    print 'add sn'
    storages[sn[0]] = (sn[1], sn[2])
    if debug : print storages

  def addCN(self,cn):
    print 'add cn'
    clients[cn[0]] = (cn[1], cn[2])
    if debug : print clients

  def delSN(self,sn):
    try:
      if debug : print 'neoclient : delSN : sn %r' %(sn)
      storages_connection[sn[0]][0].close()
      storages_connection[sn[0]][1].close()
      del storages_connection[sn[0]]
      del storages[sn[0]]
    except:
      raise
    if debug : print storages, storages_connection

  def delCN(self,cn):
    if debug : print 'del cn %r' (clients[cn[0]],)
    try:
      del clients[cn[0]]
    except:
      pass
    if debug : print clients

  def unreliableStorage(self, cn):
    pass

  def masterClose(self, mid):
    if debug : print "neoclient : Master %s closed" %mid[0]
    neoclient.replyClose (self.socket.fileno(), 28, id)
##     pid = os.getpid()
##     os.kill(pid, signal.SIGTERM)

  def masterChange(self, mn):
    if debug : print 'neoclient : new master %s, %s, %d' %(sn[0], sn[1], sn[2])

  def __del__(self):
    print 'the end'
    


class neostorage_server(Thread): 
  """ client server accepting connection from other client """

  def __init__(self):
    Thread.__init__(self)
    print '********** neostorage server init ************'
    if debug : print 'neoclient : init Client Server'
    server_started = 1
    # create server
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    global CLIENT_PORT

    while 1:
      try:
        self.sock.bind(('127.0.0.1',CLIENT_PORT))
        break
      except:
        CLIENT_PORT += 1
    if debug : print 'neoclient : client server port %d' % CLIENT_PORT
    self.sock.listen(5)
    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    

  def run(self):
    if debug : print 'neoclient : server starting..'
    # wait connection and handle it with ClientNodeServerThread
    while 1:
      conn, addr = self.sock.accept()
      thread = neostorage_server_thread(socket=conn)
      print 'thread new created'
      thread.start()
      print 'thread started'
      

  def __del__(self):
    print 'the end of server'
    self.sock.close()
##     self.stop()

  

class neostorage(BaseStorage):
  """ Storage interface for the NEO """

  def __init__(self, create=0, read_only=0, master=None):
    BaseStorage.__init__(self, 'NEO')
    self.read_only = read_only
    self.cache = cache
    # Lock for transaction
    self.txn_cond = Condition()
    # Lock for cache
    self.cache_lock = cache_lock
    # Lock for new oid
    self.oid_lock = Lock()
    # Lock for load
    self.load_lock = Lock()
    # another lock for bootstrap
    self.lock = Lock()
    # init some variable
    self.txnStorages = []
    self.txn = None
    self.tmpStore = {}
    self.serial = None
    self.master = master
    self.oidl = []
    self.__name__ = 'NEO'
    self.infos = {}
    self.infos['name'] = 'NEO (not connected)'
    self.infos['supportsVersion'] = 0
    self.infos['supportsUndo'] = 0
    self.infos['supportsTransUndo'] = 0
    self.read_only = read_only
    self.infos['extension'] = ''
    syslog.openlog('neoclient')
    self._storage = "Neo"
    self._db = None
#    gc.set_debug(gc.DEBUG_LEAK)

    # create client server
    # XXX lmuqt only creates one !!
    self.lock.acquire()
    self.server = neostorage_server()
    self.lock.release()
    self.server.start()
    
    # generate identifiant
    self.id = neoclient.generate_id()
    global id
    id = self.id
    syslog.syslog("neoclient id")
    syslog.syslog(self.id)
    if debug : print "neoclient : id %s" %self.id
    
    # connection to master...
    self.MasterSock = None
    self.Master_fd = None
    self.connected = 0;
    while not self.connected: # wait for a connection      
      self.connected = self.connectToMaster()
      time.sleep(5)
    if debug : print 'neoclient : connected to master'
    self.started = 0;
    self.lock.acquire()
    try:
      while not self.started:
        self.started = self.clientStart()
        time.sleep(15)
      if debug : print 'neoclient : client starting...'
      # get list of all clients and storages
      try:
        storageslist = neoclient.getAllSN(self.MasterSock.fileno(), 11)
        for storage in storageslist:
          storages[storage[0]] = (storage[1], storage[2])
      except SystemError:
        raise
      if debug : print storages
      try:
        clientslist = neoclient.getAllCN(self.MasterSock.fileno(), 12)
        for client in clientslist:
          if client[0] != self.id:
            clients[client[0]] = (client[1], client[2])
      except SystemError:
        raise
      if debug : print clients
    finally:
      self.lock.release()  
    if debug :
      print 'neoclient : client started'
    
  def __del__(self):
    print 'the end'
    if debug : print 'neoclient : close'
    self.close()

  def close(self):
    if debug : print 'neoclient : closing neostorage...'
    if self.MasterSock is not None:
      try:
        neoclient.clientClose(self.Master_fd, 26, self.id)
      except SystemError:
        raise
      except:
        raise
      self.MasterSock.close()
      self.MasterSock = None
##       self.server.stop()
      if debug : print 'neoclient : client closed'
      # try with self.server.stop()
      pid = os.getpid()
      os.kill(pid, signal.SIGTERM)

  def connectToMaster(self):
    try:
      # find the master
      self.master = neoclient.search_master(38)
      print 'master found on %s' %(self.master,)
      # connect to it
      self.MasterSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      self.MasterSock.connect((self.master, MASTER_PORT))
      self.Master_fd = self.MasterSock.makefile().fileno()
      return 1
    except:
      return 0

  def connectToStorage(self, id):
    """ connect to a storage """
    # A list of opened connection is keep in order to not
    # always reconnect to storages
    self.lock.acquire()
    try:
      if storages_connection.has_key(id):
        return (storages_connection[id][1]).fileno()
      if len(storages_connection) == MAX_STORAGE_CONNECTION:
        # too many connection, close one
        (fd, sock) = storages_connection.pop()
        fd.close()
        sock.close()
      # create new connection
      storage_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      storage_sock.connect((storages[id][0], storages[id][1]))
      storage_fd = storage_sock.makefile()
      storages_connection[id] = (storage_fd, storage_sock)
      if debug : print 'neoclient : connected to storage with %d' % storage_sock.fileno()
      return storage_sock.fileno()
    finally:
      self.lock.release()
    
  def closeStorageConnection(self):
    """ close all connection to storages nodes """
    global storages_connection
    print storages_connection
    for s in storages_connection:
      storages_connection[s][0].close()
      storages_connection[s][1].close()

        
  def getSnInfos(self,id):  # XXX not used
    """ get addr and port for the given storage id """
    try:
      node = neoclient.getSNInfo(self.Master_fd, 17, id)
      storages[node[0]] = (node[1], node[2])
    except:
      raise
    
  def clientStart(self):
    """ try to start and get information from the master before any work """
    if debug : print 'neoclient : client starting'
    try: 
      self.ip = socket.gethostbyname('127.0.0.1') # socket.gethostname()      
      infos = neoclient.clientStart(self.Master_fd, 31, self.id, str(self.ip), CLIENT_PORT) 
      self.infos['name'] = infos[0]
      self.infos['supportsVersion'] = infos[1]
      self.infos['supportsUndo'] = infos[2]
      self.infos['supportsTransUndo'] = infos[3]
      self.read_only = infos[4]
      self.infos['extension'] = infos[5]
      return 1
    except SystemError:
      if str(sys.exc_value) == 'temporary failure':
        print 'master node is not running'
        return 0
      else:
        raise
    except RuntimeError:
      if str(sys.exc_value) == 'master not ready':
        print 'system not ready to handle request'
        return 0
      else:
        raise
    
  def checkCache(self, objects):
    """ send list of invalidated bject to ohter client """
    if debug : print 'neoclient : checkCache'
    if len(clients) == 0:
      return
    oids = []
    for ob in objects:  # construct list of oid
      tuple = [U64(ob)]
      oids.append(tuple)
    client = []
    for c in clients: # construct list of client : (ip, port)
      tuple = [clients[c][0], int(clients[c][1])]
      client.append(tuple)
    try:
      print 'send check cache message for oids %r to clients %r' %(oids,client)
      neoclient.checkCache(19, U64(self.serial), oids, client)
    except SystemError:
      print 'system error in sending check cache'
  
  # Storage API...
  def __len__(self):
    if debug : print 'neoclient : return __len__'
    return self.cache._size
  
  def sortKey(self):
    if debug : print 'neoclient : sortkey'
    return '%s:%s' % ('Cluster Storage' , self.master)

      
  def isReadOnly(self):
    """ Storage API: return whether we are in read-only mode. """
    if debug : print 'neoclient : isReadOnly'
    return self.read_only


  def registerDB(self, db, limit):
    if debug : print 'neoclient : registerDB'
    self._db = db


  def getName(self):
    """ Storage API: return the storage name as a string. """
    if debug : print 'neoclient : getName'
    return "%s" % self.infos['name']


  def getExtensionMethods(self):
    """ StorageAPI : return extension methods support by storage. """
    if debug : print 'getExtensionMethods'
    return self.infos['extension']

  
  def supportsUndo(self):
    """ Storage API: return whether we support undo. """
    if debug : print 'neoclient : upportsUndo %d' %self.infos['supportsUndo']
    return self.infos['supportsUndo']
  

  def supportsTransactionalUndo(self):
    """ Storage API: return whether we support transactional undo."""
    if debug : print 'neoclient : support Trans Undo %d' %self.infos['supportsTransUndo']
    return self.infos['supportsTransUndo']
    

  def supportsVersions(self):
    """ Storage API: return whether we support versions. """
    if debug : print 'neoclient : supportsVersion %d' %self.infos['supportsVersion']
    return self.infos['supportsVersion']
  

  def pack(self, *d, **kw):
    if self.read_only:
      raise POSException.ReadOnlyError()    
    return
  

  def getSerial(self, oid):
    if debug : print 'neoclient : getSerial'
    self.cache_lock.acquire()
    try:
      # XXX maybe dont try in cache, may have change on master...
      if oid in self.cache:     
        return p64(long(cache[oid][0]))
    finally:
      self.cache_lock.release()
  
    try:    # otherwise get from master
      serial = neoclient.getSerial(self.Master_fd, 3, U64(oid))
      return p64(long(serial))
    except SystemError:
      raise
    except ValueError:
      raise POSException.POSKeyError(oid)


      
  def lastTransaction(self):
    """ Storage API: return last transaction id """
    if debug : 'neoclient : get last transaction'
    try:
      ltid = neoclient.getLastTransaction(self.Master_fd, 4)
      return p64(long(ltid))
    except systemError:
      raise
    

  def new_oid(self):
    """ Storage API: return a new object identifier. """
    if debug : print 'neoclient : new_oid'
    if self.read_only:
      raise POSException.ReadOnlyError()
    self.oid_lock.acquire()
    try:
      if len(self.oidl) == 0:
        # get 20 oids by default from master
        # XXX must lock to avoid many request for new oid
        self.lock.acquire()
        try:
          oid_list = neoclient.getOid(self.Master_fd, 5, 20)
        except SystemError:
          raise
        # for each oid create the string representation
        for noid in oid_list:
          oid = '\0\0\0\0\0\0\0\0'
          for i in xrange(8):
            oid = oid[:i]+chr(noid[i])
          self.oidl.append(oid)
        self.oidl.reverse()
        self.lock.release()
      self._oid =  self.oidl.pop()
      if debug : print 'neoclient : new oid %d' %U64(self._oid)  
      return self._oid
    finally:
      self.oid_lock.release()

      

  def loadSerial(self, oid, serial):
    """ Storage API: load a historical revision of an object. """
    if debug: print 'neoclient : load serial'
    self.cache_lock.acquire()
    try:       # try in cache
      if oid in self.cache:
        if self.cache[oid][0] == U64(serial):
          if debug : print 'neoclient : load serial from cache'
          return loads(self.cache[oid][1])
    finally:
      self.cache_lock.release()
    self.load_lock.acquire()
    try:
      try: # otherwise get storages from master
        load_storages = neoclient.getObjectBySerial(self.Master_fd, 2, U64(oid), U64(serial))
      except ValueError:        # serial or oid not found
        raise POSException.POSKeyError (oid,serial)
      except SystemError:
        raise
      load_storages = list(load_storages)
      while 1:
        if len(load_storages) == 0:
          # no more storage and we don't get the object
          raise RuntimeError
        s = random.choice(load_storages)
#        s = load_storages[0]
        load_storages.remove(s)
        if storages.has_key(s) is False:
          continue
        try:
          conn = self.connectToStorage(s)
          data = neoclient.load(conn, 24, U64(oid), U64(serial))
          # check crc
          if crc32(data[0]) != data[1]:
            print 'neoclient : load serial : crc check failed for oid %d' %U64(oid)
            continue
          else:
            break
        except SystemError: # XXX report failure and try with another storage        
          neoclient.failure(self.Master_fd, 18, s)
        except ValueError: # XXX not on this storage! strange but try with anther one
          continue
      # XXX must store in cache ??
      if debug : print 'neoclient : load serial from storage'
      return loads(data[0])
    finally:
      self.load_lock.release()

      

  def load(self, oid, version=None):
    """ Storage API: return the data for a given object. """
    if debug : print 'neoclient : load object %d' %U64(oid)
    self.cache_lock.acquire()
    try:
      # try in cache
      if oid in self.cache:
        if debug : print 'neoclient : load object %d from cache' %U64(oid)
        return loads(self.cache[oid][1]), p64(self.cache[oid][0])
    finally:
      self.cache_lock.release()
    # otherwise get storages from master
    self.load_lock.acquire()
    try:
      try:
        data = neoclient.getObjectByOid(self.Master_fd, 1, U64(oid))
      except ValueError:
        if debug : print 'neoclient : load -> object not found'
        raise POSException.POSKeyError (oid)
      except SystemError:
        raise
      # get data from storage
      serial = data[0]
      load_storages = list(data[1:])
      while 1:
        if len(load_storages) == 0:
          # no more storage and we don't get the object
          raise RuntimeError
        s = random.choice(load_storages)
        load_storages.remove(s)          
        if storages.has_key(s) is False:
          continue
        try:
          conn = self.connectToStorage(s)
          data = neoclient.load(conn, 24, U64(oid), serial)
          # check crc
          if crc32(data[0]) != data[1]:
            print 'neoclient : error in crc check for object %d' %U64(oid)
            continue # XXX maybe report error ??
          else:
            break
        except ValueError:
          continue
        except SystemError:
          neoclient.failure(self.Master_fd, 18, s)
      # store object in cache
      self.cache_lock.acquire()
      cache[oid] = (serial, data[0])
      self.cache_lock.release()
      return loads(data[0]), p64(serial)
    finally:
      self.load_lock.release()
    

  def getSize(self):
    """ Storage API: an approximate size of the database, in bytes. """
    if debug : print 'neoclient : getsize'
#     size = 0
#     for storage in storages:
#       try:
#         conn = self.connectToStorage(storage)
#         size += neoclient.getSize (conn, 25)
#       except SystemError: # notify failure to master
#         neoclient.failure(self.Master_fd, 18, storage)
#       except:
#         continue
#     if size == 0:
#       return self.cache._size    
#     return size
    return self.cache._size    


  def undo(self, *d, **kw):
    """ Deprecated, use undoTransactional instead"""
    if debug : print 'neoclient : undo'
    if self.read_only:
      raise POSException.ReadOnlyError()
    raise POSException.UndoError, 'non-undoable transaction'

      
  def undoLog(self, *d, **kw):
    """ Deprecated, use undoInfo instead """
    if debug : print 'neoclient : undo log'
    return ()
  
  
  def undoInfo(self, first=0, last=-20, specification=None):
    """ Storage API: return undo information. """
    if debug : print 'neoclient : undo info, first %d, last %d' %(first, last)
    try:
      tlist = neoclient.getTransSN(self.Master_fd, 6, first, last)
    except SystemError:
      raise
    undoInfo = []
    for trans in tlist:
      undo_storages = list(trans[1:])
      while 1:
        if len(undo_storages) == 0: 
          break # or raise error
        s = random.choice(undo_storages) # choose a random storage and remove it from list
        undo_storages.remove(s)          
        try:
          conn = self.connectToStorage(s)
          data = neoclient.undoInfo(conn, 23, trans[0])
          info = {} 
          info['time'] = TimeStamp(p64(long(data[3]))).timeTime()
          info['username'] = data[1]
          info['description'] = data[2]
          info['id'] = p64(long(data[3]))
          undoInfo.append(info)
          break
        except SystemError:
          neoclient.failure(self.Master_fd, 18, s)
        except ValueError:
          raise POSException.UndoError('invalid transaction id', trans[0])
    return undoInfo
  

  def history(self, oid, version=None, length=1):
    """ Storage API: return a sequence of HistoryEntry objects. """
    if debug : print 'neoclient : history'
    histInfo = []
    try:
      t = time.time() 
      slist = neoclient.getObjectHist(self.Master_fd, 7, U64(oid), length)
      t = time.time() - t
      print 'neoclient : master time : %.4f' %t
    except SystemError:
      raise
    except ValueError:
      raise POSException.POSKeyError(oid)
    for serial in slist:
      hist_storages = list(serial[1:])
      while 1:
        if len(hist_storages) == 0: 
          break # or raise error
        s = random.choice(hist_storages) # choose a random storage and remove it from list
        # s = hist_storages[0]
        if debug : print 'get info from %s for %s' %(s, serial[0])
        hist_storages.remove(s)
        try:
          conn = self.connectToStorage(s)
          t = time.time()
          data = neoclient.histInfo(conn, 22, U64(oid), serial[0])
          t = time.time() - t
          print 'storage time : %.4f' %t
          hist = {} 
#           hist['time'] = float(data[0])
          hist['time'] = TimeStamp(p64(long(data[3]))).timeTime()
          hist['user_name'] = data[1]
          hist['description'] = data[2]
          hist['serial'] = p64(long(data[3]))
          hist['version'] = None
          hist['size'] = data[4]
          histInfo.append(hist)
          break
        except SystemError:
          neoclient.failure(self.Master_fd, 18, s)
        except ValueError:
          continue
    return histInfo
  

  # method following are for transaction
  def transactionalUndo(self, tid, txn=None):
    """ Storage API: undo a transaction in context of a transaction.  """
    if debug :
      print 'neoclient : transactional undo'
    if self.read_only:
      raise POSException.ReadOnlyError()
    if txn is not self.txn:
      raise POSException.StorageTransactionError(self, transaction)
    # XXX think we must only mark the transaction as undone onto the master
    # and let it remaining on storage node
    try:
      # get storages from master
      try:
        undo_storages = neoclient.undoTrans(self.Master_fd, 8, U64(tid));
      except ValueError:         # trans not found
        raise POSException.UndoError (tid)
      except SystemError:
        raise
      # undo trans on storages
      undo_storages = list(undo_storages)
      for s in undo_storages:
        try:
          conn = self.connectToStorage(s)
          data = neoclient.undo(conn, 21, U64(tid))
          break
        except SystemError:
          neoclient.failure(self.Master_fd, 18, s)
        except ValueError:
          continue
      # must check its own and the other client cache
      oid_list = []
      self.cache_lock.acquire()
      for oid in data:
        # invalidate cache
        if p64(oid) in self.cache:
          print 'neoclient : del in cache'
          del cache[p64(oid)]
        # construct list of oid for zope
        oid_list.append(p64(oid))          
      self.cache_lock.release()
      return oid_list
    finally:
      pass


  def tpc_begin(self, transaction, tid=None, status=' '):
    """ Storage API: begin a transaction. """    
    if debug :
      print 'neoclient : tpc_begin'
    if self.read_only:
      raise POSException.ReadOnlyError()
    # get transaction
    self.txn_cond.acquire()
    while self.txn is not None:
      if self.txn == transaction:
        self.txn_cond.release()
        return
      self.txn_cond.wait(30)
    self.txn = transaction
    self.txn_cond.release()
    # begin it
    try:
      data = neoclient.beginTrans(self.Master_fd, 9, 0);
    except SystemError:
      raise
    # generate the new tid if no tid given
    if tid is None:
      tid =  '\0\0\0\0\0\0\0\0'
      for i in xrange(8): # construct tid in 8 byte-string
        tid = tid[:i]+chr(data[0][i])
    self.serial = self._ts = tid # tid    
    self.txnStorages = data[1:] # list of storages
    if debug :
      print "neoclient : begin transaction %s" %U64(self.serial)
      print self.txnStorages

      #    return self.serial


  def store(self, oid, serial, data, version, transaction):
    """Storage API: store data for an object."""
    if debug : print 'neoclient : store, oid %d' %U64(oid)
    if self.read_only:
      raise POSException.ReadOnlyError()
    if self.txn is not transaction:
      raise POSException.StorageTransactionError(self, transaction)
    # store object in temporary buffer
    if serial is None:
      serial = '\0\0\0\0\0\0\0\0'
    if oid in self.cache:
      serial = p64(self.cache[oid][0])
    self.tmpStore[oid] = (U64(serial), dumps(data))
    return serial



  def tpc_vote(self, transaction): 
    """Storage API: vote on a transaction."""
    if debug : print 'neoclient : tpc_vote'
    if self.read_only:
      raise POSException.ReadOnlyError()
    if self.txn is not transaction:
      raise POSException.StorageTransactionError(self, transaction)
    # construct list of object to be stored
    arg = []
    size = 0
    for ob in self.tmpStore:
      crc = crc32(self.tmpStore[ob][1])
      tuple = [U64(ob), U64(self.serial), self.tmpStore[ob][1], crc]
      #        oid         , serial               , data      , crc
      size += len(self.tmpStore[ob][1]) # data size
      arg.append (tuple)
    # check if transaction argument are not None otherwise put empty string instead
    if self.txn.user is None:
      self.txn.user = ""
    if self.txn.description is None:
      self.txn.description = ""
    if self.txn._extension is None:
      self.txn._extension = ""
    # send objects in transaction to all storages nodes
    for s in self.txnStorages:      
      try:
        if debug :
          print 'neoclient :  send transaction to storage %s, %d' %(storages[s][0], storages[s][1])
        # send transaction to storage
        conn = self.connectToStorage(s)
        neoclient.transaction(conn, 20, self.txn.user, self.txn.description, self.txn._extension, U64(self.serial), size, arg)
        if debug : print "neoclient : transaction stored!"
      except SystemError:
        neoclient.failure(self.Master_fd, 18, s)
        continue
      except RuntimeError: # must abort transaction
        print 'RuntTimeError in tpc_vote for storage '


    # construct list of oid-serial for validation
    oid = []    
    for ob in self.tmpStore:
      tuple = [U64(ob)]
      # must send the last serial of the object in order
      # to make the transaction validated by master
      if self.tmpStore[ob][0] is None:
        tuple.append(" ")
      else:
        tuple.append(self.tmpStore[ob][0])
      oid.append(tuple)
    # construct list of storages
    storage = []
    for s in self.txnStorages:
      tuple = [s]
      storage.append(tuple)
    # validate transaction by master
    try:
      neoclient.endTrans(self.Master_fd, 10, U64(self.serial), oid, storage)
      if debug : print 'neoclient : transaction validated!!'
    except RuntimeError: # transaction not validated by master, must abort...
      print 'RuntTimeError in tpc_vote for master'
      raise POSException.ReadConflictError
    except SystemError:
      print 'SystemError in tpc_vote for master'
      raise
    return # XXX return nothing otherwise ZODB raise assertion error in check serial



  def tpc_finish(self, transaction, f=None):
    """ Storage API: finish a transaction. """
    if debug : print 'neoclient : tpc_finish'
    self.load_lock.acquire()
    if self.read_only:
      raise POSException.ReadOnlyError()
    if self.txn is not transaction:
      raise POSException.StorageTransactionError(self, transaction)
    try:
      if f is not None:
        f()
      # update cache
      self.cache_lock.acquire()
      for ob in self.tmpStore:
        cache[ob] = (U64(self.serial), self.tmpStore[ob][1]) # put data in cache
      self.cache_lock.release()
      # invalidate other clients cache
      try:
        self.checkCache(self.tmpStore)
      except:
        raise
      # reset transaction data
      self.txnStorages = []
      self.tmpStore = {}
      self.serial = None
      self.txn_cond.acquire()
      self.txn = None
      self.txn_cond.notify()
      self.txn_cond.release()
      if debug : print 'neoclient : transaction ended!, txn = %r' %(self.txn,)
      return 
    finally:
      self.load_lock.release()

  def tpc_abort(self, transaction):
    """ Storage API: abort a transaction. """
    if debug :
      print 'neoclient : tpc_abort'
    if self.read_only:
      raise POSException.ReadOnlyError()
    if self.txn is not transaction:
      raise POSException.StorageTransactionError(self, transaction)
    # called because vote failed, so just do undoTrans on storages, master already known failure
    for s in self.txnStorages:
      try:
        conn = self.connectToStorage(s)
        list = neoclient.undo(conn, 21, U64(self.serial))
      except SystemError:
        neoclient.failure(self.Master_fd, 18, s)      
      except ValueError:
        print 'abort : Value Error'
        raise 
    # clear data for transaction
    self.txn_cond.acquire()
    self.txnStorages = []
    self.tmpStore = {}
    self.serial = None
    self.txn = None
    self.txn_cond.notify()
    self.txn_cond.release()

    

  # all methods about version, we don't care
  def versionEmpty(self, version):
    """ Storage API: return whether the version has no transactions. """
    if debug : print 'neoclient : version empty'
    return 1

  def versions(self, max=None):
    """ Storage API: return a sequence of versions in the storage. """
    if debug : print 'neoclient : versions'
    return ()

  def abortVersion(self, version, transaction):
    """ Storage API: clear any changes made by the given version. """
    if debug : print 'neoclient : abortversion'
    return ()

  def commitVersion(self, source, destination, transaction):
    """ Storage API: commit the source version in the destination. """
    if debug : print "neoclient : commit Version"
    return ()
    
  def modifiedInVersion(self, oid):
    """ Storage API: return the version, if any, that modfied an object. """
    if debug : print ' neoclient : modified in version'
    return ''
