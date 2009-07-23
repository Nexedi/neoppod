from mock import Mock
import logging
connector_cpt = 0
from neo.protocol import Packet, INVALID_UUID, MASTER_NODE_TYPE

import os

# master node with the highest uuid will be declared as PMN
previous_uuid = None
def getNewUUID():
    uuid = INVALID_UUID
    previous = globals()["previous_uuid"]
    while uuid == INVALID_UUID or (previous is \
              not None and uuid > previous):
        uuid = os.urandom(16)
    logging.info("previous > uuid %s"%(previous > uuid)) 
    globals()["previous_uuid"] = uuid
    return uuid


class DoNothingConnector(Mock):
  def __init__(self, s=None):
    logging.info("initializing connector")
    self.desc = globals()['connector_cpt']
    globals()['connector_cpt'] = globals()['connector_cpt']+ 1
    self.packet_cpt = 0
    Mock.__init__(self)

  def getAddress(self):
    return self.addr
  
  def makeClientConnection(self, addr):
    self.addr = addr

  def getDescriptor(self):
    return self.desc


class TestElectionConnector(DoNothingConnector):
  def receive(self):
    """ simulate behavior of election """
    if self.packet_cpt == 0:
      # first : identify
      logging.info("in patched analyse / IDENTIFICATION")
      p = Packet()
      self.uuid = getNewUUID()
      p.acceptNodeIdentification(1,
                                 MASTER_NODE_TYPE,
                                 self.uuid,
                                 self.getAddress()[0],
                                 self.getAddress()[1],
                                 1009,
                                 2
                                 )
      self.packet_cpt += 1
      return p.encode()
    elif self.packet_cpt == 1:
      # second : answer primary master nodes
      logging.info("in patched analyse / ANSWER PM")
      p = Packet()
      p.answerPrimaryMaster(2,
                            INVALID_UUID,
                            []
                            )        
      self.packet_cpt += 1
      return p.encode()
    else:
      # then do nothing
      from neo.connector import ConnectorTryAgainException
      raise ConnectorTryAgainException
    
