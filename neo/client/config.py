from ZODB.config import BaseConfig

class NeoStorage(BaseConfig):

  def open(self):
    from Storage import Storage
    return Storage(master_nodes = self.config.master_nodes, name = self.config.name)


