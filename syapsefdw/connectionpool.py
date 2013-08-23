import syapse_client

from .utils import pg_log

class Singleton(type):
    def __init__(cls, name, bases, dict):
        super(Singleton, cls).__init__(name, bases, dict)
        cls.instance = None 

    def __call__(cls,*args,**kw):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args, **kw)
        return cls.instance

class ConnectionPool(object):
    """Connection pool for Syapse connections"""
    __metaclass__ = Singleton

    def __init__(self):
        self.pool = dict()

    def get(self,hostname,email,password):
        hep = (hostname,email,password)
        if hep not in self.pool:
            self.pool[hep] = syapse_client.SyapseConnection(hostname,email,password)
            pg_log('connected to Syapse ({hostname}/{email}:***)'.format(
                    hostname = hostname, email = email))
        return self.pool[hep]

connection_pool = ConnectionPool()
