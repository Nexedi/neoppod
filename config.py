from ConfigParser import SafeConfigParser
import logging

class ConfigurationManager:
    """This class provides support for parsing a configuration file."""

    # The default values for a config file.
    default_config_dict = {'database' : 'test',
                           'user' : 'test',
                           'password' : None,
                           'server' : '127.0.0.1',
                           'master_nodes' : '',
                           'replicas' : '1',
                           'partitions' : '1009',
                           'name' : 'main'}

    # The default port to which master nodes listen when none is specified.
    default_master_port = 10100

    def __init__(self, file, section):
        parser = SafeConfigParser(self.default_config_dict)
        logging.debug('reading a configuration from %s', file)
        parser.read(file)
        self.parser = parser
        self.section = section

    def __getitem__(self, key):
        return self.parser.get(self.section, key)

    def getDatabase(self):
        return self['database']

    def getUser(self):
        return self['user']

    def getPassword(self):
        return self['password']

    def getServer(self):
        server = self['server']
        if ':' in server:
            ip_address, port = server.split(':')
            port = int(port)
        else:
            ip_address = server
            port = self.default_master_port
        return ip_address, port

    def getReplicas(self):
        return int(self['replicas'])

    def getPartitions(self):
        return int(self['partitions'])

    def getName(self):
        return self['name']

    def getMasterNodeList(self):
        master_nodes = self['master_nodes']
        master_node_list = []
        # A list of master nodes separated by whitespace.
        for node in master_nodes.split():
            if not node:
                continue
            if ':' in node:
                ip_address, port = node.split(':')
                port = int(port)
            else:
                ip_address = node
                port = self.default_master_port
            master_node_list.append((ip_address, port))
        return master_node_list
        
