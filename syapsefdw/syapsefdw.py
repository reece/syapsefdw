import ConfigParser 

from multicorn import ForeignDataWrapper

from connectionpool import ConnectionPool, connection_pool

class SyapseFDW(ForeignDataWrapper):
    """A Table is an abstract object that represents a source of data from
    Syapse. It includes the properties-to-column mapping. Subclasses
    implement the essential `execute' method for FDWs that depend on
    source.  Subclasses also generate the DDL used to declare foreign
    tables in PostgreSQL."""
    def __init__(self, options, columns={}):
        super(SyapseFDW, self).__init__(options, columns)
        self.options = options
        self.columns = columns
        self.conf = ConfigParser.SafeConfigParser()
        self.conf.readfp( open(self.options['conf_file'],'r') )
        self.conn = connection_pool.get(self.conf.get('syapse','hostname'),
                                        self.conf.get('syapse','email'),
                                        self.conf.get('syapse','password'))

    def execute(self, quals, columns):
        raise NotImplemented('execute method must be subclassed')

    def table_ddl(self):
        cols = [ '{cd.column:30} {cd.type:10} -- {cd.syapse_cardinality:10}; {cd.syapse_type:10}; {cd.syapse_property}'.format(cd=cd)
                 for cd in self.coldefs ]
        return """DROP FOREIGN TABLE {tablename};
CREATE FOREIGN TABLE {tablename} (
    {cols}
) server syapse options (
  syapse_hostname '{self.options[syapse_hostname]}',
  syapse_email    '{self.options[syapse_email]}',
  syapse_password '{self.options[syapse_password]}',
  source      	  '{self.options[source]}'
);
""".format( tablename = self.tablename,
            cols = "\n  , ".join(cols),
            self=self )

