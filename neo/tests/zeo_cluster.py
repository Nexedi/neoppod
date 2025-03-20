from __future__ import print_function
import os
import signal
import tempfile
import ZEO.runzeo
from ZEO.ClientStorage import ClientStorage as _ClientStorage
from . import buildUrlFromString, ADDRESS_TYPE, IP_VERSION_FORMAT_DICT
from .functional import AlreadyStopped, PortAllocator, Process

class ZEOProcess(Process):

    def __init__(self, **kw):
        super(ZEOProcess, self).__init__('runzeo', **kw)

    def run(self):
        from ZEO.runzeo import ZEOServer
        del ZEOServer.handle_sigusr2
        getattr(ZEO, self.command).main()

class ClientStorage(_ClientStorage):

    @property
    def restore(self):
        raise AttributeError('IStorageRestoreable disabled')

class ZEOCluster(object):

    def start(self):
        self.zodb_storage_list = []
        local_ip = IP_VERSION_FORMAT_DICT[ADDRESS_TYPE]
        port_allocator = PortAllocator()
        port = port_allocator.allocate(ADDRESS_TYPE, local_ip)
        self.address = buildUrlFromString(local_ip), port
        temp_dir = tempfile.mkdtemp(prefix='neo_')
        print('Using temp directory', temp_dir)
        self.zeo = ZEOProcess(address='%s:%s' % self.address,
                              filename=os.path.join(temp_dir, 'Data.fs'))
        port_allocator.release()
        self.zeo.start()

    def stop(self):
        storage_list = self.zodb_storage_list
        zeo = self.zeo
        del self.zeo, self.zodb_storage_list
        try:
            for storage in storage_list:
                storage.close()
            zeo.kill(signal.SIGUSR2)
        except AlreadyStopped:
            pass
        else:
            zeo.child_coverage()
            zeo.kill(signal.SIGKILL)
            zeo.wait()

    def getZODBStorage(self):
        storage = ClientStorage(self.address)
        self.zodb_storage_list.append(storage)
        return storage
