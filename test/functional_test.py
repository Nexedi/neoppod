#! /usr/bin/env python

#
# Functional test script for the neo system
# See how it works if we get storage failure and recovery
#

import os, time, sys, signal, random
from ZODB import Transaction
from NEO import neostorage
from threading import Thread, Lock

login = 'aurelien'
config_file = "neo.conf"

class foo:
  """ class for testing object """
  pass


NUM_CLIENTS = 20
NUM_THREADS_PER_CLIENT = 5
NUM_REQUESTS = 20
NUM_OBJECTS_PER_TRANS = 1
OBJECT_DATA = "data for objects..."
NO_VERSION = 0
CRASH_FREQUENCY =400 # frequency of crash relative to the number of requests

storages_list = {}
objects_list = []
user_thread_list = []
client_thread_list = []
request_cpt = 0
neomaster_ip = None
cpt_lock = Lock()

# functions for managing nodes
def setup_nodes():
  """ setup master and storage nodes from the config file """
  global neomaster_ip
  f = open(config_file, "r")
  line = f.readline()
  while line != '':
    # comment line
    if line[0] == '#':
      line = f.readline()
      continue
    # read config line
    line = line.split(' ', 4)
    type = line[0]
    ip = line[1]
    exec_path = line[2]
    path_to_bin = line[3]
    options = line[4]
    if options[-1] == '\n':
      options = options[:-1]
    # setup the node
    if type == 'master':
      neomaster_ip = ip
      os.system('ssh '+login+'@'+ip+' "cd '+exec_path+'; '+path_to_bin+'neomaster '+options+'" &')
    if type == 'storage':
      storages_list[len(storages_list)] = (ip, exec_path, path_to_bin, options)
      os.system('ssh '+login+'@'+ip+' "cd '+exec_path+'; '+path_to_bin+'neostorage '+options+' -m'+neomaster_ip+'" &')
    # wait for init
    time.sleep (1)
    line = f.readline()

def crash_node(node):
  """ kill a node """
  os.system('ssh '+login+'@'+str(node[0])+' killall '+str(node[2])+'neostorage -sSEGV &')
  print 'functional test : on node %s, storage crashed' %(node[0])

def restart_node(node):
  """ restart a node """
  global neomaster_ip
  os.system('ssh '+login+'@'+str(node[0])+' "cd '+str(node[1])+'; '+str(node[2])+'neostorage -l3 -m'+neomaster_ip+' &>/dev/null" &')
  print 'functional test : storage on node %s restarted' %(node[0])

def close_master():
  """ when master node close, all storages nodes are close automatically """
  global neomaster_ip
  os.system('ssh '+login+'@'+neomaster_ip+' killall neomaster')

# functions for managing ZODB behaviour
def createTransaction():
  """ create a zodb transaction """
  txn = Transaction.Transaction()
  txn.user = "test user"
  txn.description = "transaction for functional tests"
  txn.extension = "None"
  return txn

def storeObjects(db):
  """ store objects into database """
  t = createTransaction()
  tid = db.tpc_begin(t)
  for i in xrange(NUM_OBJECTS_PER_TRANS):
    oid = db.new_oid()
    objects_list.append(oid)
    obj = foo()
    obj.data = OBJECT_DATA
    db.store(oid, None, obj, 0, t)
  db.tpc_vote(t)
  db.tpc_finish(t)


# functions for simulating users behaviour
def simulate_neo_user(db=None):
  """ simulate a user storing some objects """
  global request_cpt
  for i in xrange(NUM_REQUESTS):
    cpt_lock.acquire()
    request_cpt+=1
    print 'simulate_neo_user : i %r, db %r, request_cpt %r' %(i, db, request_cpt)
    cpt_lock.release()
    storeObjects(db)
    # check if we have to crash a node
    cpt_lock.acquire()    
    if (request_cpt % CRASH_FREQUENCY) == 0:
      cpt_lock.release()
      print 'crashing node...'
      # XXX may find a better way to do this
      s = random.choice (storages_list)    
      crash_node(s)
      time.sleep(2)
      restart_node(s)
    else:
      cpt_lock.release()


def simulate_neo_client():
  """ simulate a  neoclient wich handle many connection from users """
  db = neostorage.neostorage(master = neomaster_ip)
  # each client have some users
  for i in xrange(NUM_THREADS_PER_CLIENT):
    th = Thread(target=simulate_neo_user, args=(db,))
    th.start()
    user_thread_list.append(th)    
  for th in user_thread_list:
    th.join()
  # try to load all objects created by users
  for oid in objects_list:
    object, serial = db.load(oid, NO_VERSION)
    assert object.data == OBJECT_DATA, 'different data for object'


if __name__ == '__main__':

  setup_nodes()
  
  for i in xrange(NUM_CLIENTS):
    th = Thread(target=simulate_neo_client, args=())
    th.start()
    client_thread_list.append(th)
  for th in client_thread_list:
    th.join()

  close_master()
