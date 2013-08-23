import syapse_client

from classtable import ClassTable
from savedquerytable import SavedQueryTable
from utils import pg_log

class SyapseFDWFactory(object):
    """This class creates new subclasses of SyapseFDW based on the
    provided arguments to the constructor.

    Unlike traditional factories, this factory creates new instances of
    other classes through its constructor rather than through a dedicated
    method.  You will never get an instance of SyapseFDWFactory itself.

    Why?  Multicorn setup requires calling a class constructor for the
    FDW.  When multiple subclasses are used to handle FDWs for a single
    server, it's convenient to be able to instantiate subclasses based on
    arguments."""
    def __new__(cls,options,columns):
        return create_fdw(options,columns)

def create_fdw(options,columns):
    """simple factory to make different FDW depending on source"""
    cl = ClassTable
    if options.get('source').startswith('fdw:'):
        cl = SavedQueryTable
    fdw = cl(options,columns)
    pg_log('created '+str(type(fdw))+' for '+options.get('source'))
    return fdw

