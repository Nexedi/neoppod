from ZODB.config import BaseConfig

class neostorage(BaseConfig):

  def open(self):
    from neostorage import neostorage
    return neostorage(master = self.config.master)

                  
