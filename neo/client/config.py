from ZODB.config import BaseConfig

class NEOStorage(BaseConfig):

  def open(self):
    from NEOStorage import NEOStorage
    return NEOStorage(master_nodes = self.config.master_nodes, name = self.config.name)


