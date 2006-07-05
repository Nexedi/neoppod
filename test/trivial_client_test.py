#! /usr/bin/env python

#
# Some functions to test quickly basics functions of neo
#
from NEO import neostorage
from ZODB import POSException, Transaction
from ZODB.utils import oid_repr, p64, U64

oid1 = ''
serial1 = ''


class foo:
  """ class representing an object """
  pass

def store_objects(c):
  """ store some objects in one transaction """  
  global oid1, serial1
  oid1 = c.new_oid()
  oid2 = c.new_oid()
  oid3 = c.new_oid()  
  print "apply store_object : new oid %s %s" %(U64(oid1), oid_repr(oid1))
  # try to load an object
  try:
    ob = c.load(oid1, 0)
  except POSException.POSKeyError:
    print 'apply store_object : object not found'
  # store an object using transaction
  txn = Transaction.Transaction()
  txn.user = "moi"
  txn.description = "transaction test"
  txn.extension = "no extension"
  serial = c.tpc_begin(txn)
  serial1 = serial
  print "apply store_object : serial %s"  %oid_repr(serial)
  # first object
  ob1 = foo()
  ob1.txt = "object data for testing"
  c.store(oid1, None, ob1, 0, txn)
  # try with second object
  ob2 = foo()
  ob2.txt = "second object data for testing"
  c.store(oid2, None, ob2, 0, txn)
  # try with third object
  ob3 = foo()
  ob3.txt = "third object data for testing"
  c.store(oid3, None, ob3, 0, txn)
  # make transaction
  c.tpc_vote(txn)
  c.tpc_finish(txn)
  # now undo trans
#   c.transactionalUndo (serial)
  txn = None  
  print "apply store_object : transaction finished"
  c.load(oid1, 0)

def update_object(c):
  """ update one object already stored """
  # load object
  ob = c.load(oid1, 0)
  print 'apply update_object : object load with data : %s' %ob.txt
  # create transaction
  txn = Transaction.Transaction()
  txn.user = "moi"
  txn.description = "transaction test for update"
  txn.extension = "no extension"
  serial = c.tpc_begin(txn)
  # modify it
  print 'apply update_object : transaction serial %s' %U64(serial)
  ob.txt = "data updated for object"
  # store transaction
  oldserial = c.getSerial(oid1)
  print oid_repr(oldserial), oid_repr(oldserial)
  c.store(oid1, oldserial, ob, 0, txn)
  c.tpc_vote(txn)
  c.tpc_finish(txn)
  print 'apply update_object : update transaction ended'


def load_last(c):
  """ load last version of object """
  ob = c.load(oid1, 0)
  print 'apply load_last : last object load with data : %s' %ob.txt

def load_old(c):
  """ load an old version of object """
  ob = c.loadSerial(oid1,serial1)
  print 'apply load_old : old object load with data : %s' %ob.txt

def get_serial(c):
  """ get serial of an objet and load it """
  serial = c.getSerial(oid1)
  print 'apply get_serial : serial get %s' %U64(serial)
  ob = c.loadSerial (oid1, serial)
  print 'apply get_serial : load serial, data : %s' %ob.txt

def lastTrans(c):
  """ get last transaction id """
  ltid = c.lastTransaction()
  print 'apply lastTrans : last transaction id : %s' %U64(ltid)

def transInfo(c):
  """ get info about the two last transaction """
  infos = c.undoInfo(last=-2)
  print "apply transInfos : "
  print infos
  
def objectHistory(c):
  """ get history for one object """
  hist = c.history(oid1, length = 2)
  print "apply objectHistory : "
  print hist

def abortInTrans(c):
  """ abort transaction before finished, called after vote """
  oid1 = c.new_oid()
  oid2 = c.new_oid()
  print "apply : new oid %s %s" %(oid_repr(oid1), oid_repr(oid2))
  # store an object using transaction
  txn = Transaction.Transaction()
  txn.user = "moi"
  txn.description = "transaction test"
  txn.extension = "no extension"
  serial = c.tpc_begin(txn)
  print "apply : serial %s"  %oid_repr(serial)
  # first object
  ob1 = foo()
  ob1.txt = "object data for testing"
  c.store(oid1, serial, ob1, 0, txn)
  # try with second object
  ob2 = foo()
  ob2.txt = "second object data for testing"
  c.store(oid2, serial, ob2, 0, txn)
  # make transaction
  c.tpc_vote(txn)
  c.tpc_abort(txn)
  txn = None  
  print "apply : transaction aborted"    


def transUndo(c):
  """ Test the transactional undo method """
  oid1 = c.new_oid()
  oid2 = c.new_oid()
  print "undo : apply : new oid %s %s" %(oid_repr(oid1), oid_repr(oid2))
  # store an object using transaction
  txn = Transaction.Transaction()
  txn.user = "moi"
  txn.description = "undo transaction test"
  txn.extension = "no extension"
  serial = c.tpc_begin(txn)
  print "apply : serial %s"  %oid_repr(serial)
  # first object
  ob1 = foo()
  ob1.txt = "object data for testing"
  c.store(oid1, serial, ob1, 0, txn)
  # try with second object
  ob2 = foo()
  ob2.txt = "second object data for testing"
  c.store(oid2, serial, ob2, 0, txn)
  # make transaction
  c.tpc_vote(txn)
  c.transactionalUndo(serial, txn)
  print 'apply : trans undo done'
  



if __name__ == '__main__':
  # instantiate storage and run some test
  c = neostorage.neostorage (master = '127.0.0.1')
  
  store_objects(c)    
  update_object(c)     
  load_last(c)
  load_old(c)
  get_serial(c)
  lastTrans(c)
  transInfo(c)
#   transUndo(c)  
#   abortInTrans(c)
#   objectHistory(c)  
#   c.close()

