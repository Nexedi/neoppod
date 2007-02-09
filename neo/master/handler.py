import logging

from neo.handler import EventHandler

class MasterEventHandler(EventHandler):
    """This class implements a generic part of the event handlers."""
    def __init__(self, app):
        self.app = app
        EventHandler.__init__(self)

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, ip_address, port, name):
        raise NotImplementedError('this method must be overridden')

    def handleAskPrimaryMaster(self, conn, packet):
        raise NotImplementedError('this method must be overridden')

    def handleAnnouncePrimaryMaster(self, conn, packet):
        raise NotImplementedError('this method must be overridden')

    def handleReelectPrimaryMaster(self, conn, packet):
        raise NotImplementedError('this method must be overridden')

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        logging.info('ignoring Notify Node Information')
        pass

    def handleAnswerLastIDs(self, conn, packet, loid, ltid, lptid):
        logging.info('ignoring Answer Last IDs')
        pass

    def handleAnswerPartitionTable(self, conn, packet, ptid, cell_list):
        logging.info('ignoring Answer Partition Table')
        pass

    def handleAnswerUnfinishedTransactions(self, conn, packet, tid_list):
        logging.info('ignoring Answer Unfinished Transactions')
        pass

    def handleAnswerTransactionInformation(self, conn, packet, tid,
                                           user, desc, ext, oid_list):
        logging.info('ignoring Answer Transactin Information')
        pass

    def handleTidNotFound(self, conn, packet, message):
        logging.info('ignoring Answer OIDs By TID')
        pass

    def handleAnswerObjectPresent(self, conn, packet, oid, tid):
        logging.info('ignoring Answer Object Present')
        pass

    def handleOidNotFound(self, conn, packet, message):
        logging.info('ignoring OID Not Found')
        pass

    def handleAskNewTID(self, conn, packet):
        logging.info('ignoring Ask New TID')
        pass

    def handleAskNewOIDs(self, conn, packet):
        logging.info('ignoring Ask New OIDs')
        pass

    def handleFinishTransaction(self, conn, packet, oid_list, tid):
        logging.info('ignoring Finish Transaction')
        pass

    def handleNotifyInformationLocked(self, conn, packet, tid):
        logging.info('ignoring Notify Information Locked')
        pass

    def handleAbortTransaction(self, conn, packet, tid):
        logging.info('ignoring Abort Transaction')
        pass

    def handleAskLastIDs(self, conn, packet):
        logging.info('ignoring Ask Last IDs')
        pass

    def handleAskUnfinishedTransactions(self, conn, packet):
        logging.info('ignoring ask unfinished transactions')
        pass
