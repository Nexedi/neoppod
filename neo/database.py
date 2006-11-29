import MySQLdb
from MySQLdb import OperationalError
from MySQLdb.constants.CR import SERVER_GONE_ERROR, SERVER_LOST
import logging

class DatabaseManager:
    """This class manages a database.

    For now, this implementation is limited to MySQL."""

    def __init__(self, database, user, password = None):
        self.db = database
        self.user = user
        self.passwd = password
        self.conn = None
        self.connect()

    def connect(self):
        kwd = {'db' : self.db, 'user' : self.user}
        if self.passwd is not None:
            kwd['passwd'] = self.passwd
        logging.info('connecting to MySQL on the database %s with user %s',
                     self.db, self.user)
        self.conn = MySQLdb.connect(**kwd)

    def query(self, query):
        """Query data from a database."""
        conn = self.conn
        try:
            logging.debug('querying %s...', query.split('\n', 1)[0])
            conn.query(query)
            r = conn.store_result()
            if r is not None:
                r = r.fetch_row(r.num_rows())
        except OperationalError, m:
            if m[0] in (SERVER_GONE_ERROR, SERVER_LOST):
                logging.info('the MySQL server is gone; reconnecting')
                self.connect()
                return self.query(query)
            raise
        return r
 
