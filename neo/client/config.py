from ZODB.config import BaseConfig

class NEOStorage(BaseConfig):

  def open(self):
    from NEOStorage import NEOStorage
    return NEOStorage(master_addr = self.config.master_addr, master_port = int(self.config.master_port), name = self.config.name)


