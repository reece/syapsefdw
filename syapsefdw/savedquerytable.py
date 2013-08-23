import syapse_client

from .syapsefdw import SyapseFDW
from .coldef import ColDef
from .utils import map_values, transform_value, savedquery_to_tablename, camelcase_to_underscore

class SavedQueryTable(SyapseFDW):
    def __init__(self,options,columns):
        super(SavedQueryTable, self).__init__(options,columns)
        self.conn._sqs = dict( self.conn.getAllSavedQueries() ) # add to conn object, shared across conns
        self.syapse_saved_query = options['source']
        self.syapse_saved_query_id = self.conn._sqs[ self.syapse_saved_query ]
        self.tablename = savedquery_to_tablename(self.syapse_saved_query)
        self._build_coldefs()

    def execute(self, quals, columns):
        def _process_ds_row(coldefs,ds_row):
            # horrors. The AII is buried in "AnnotatedValue" records,
            # which are returned for system properties when the saved
            # query is executed with annotate_meta=True.  These AV
            # instances happen to contain the AII for the current
            # record. We fish the AII out of any of these AVs.
            avs = [ v for v in ds_row 
                    if isinstance(v,syapse_client.sem.advq.AnnotatedValue) ]
            aii = avs[0].app_ind_id if len(avs) > 0 else None
            vals = [ v.value if isinstance(v,syapse_client.sem.advq.AnnotatedValue) else v
                     for v in [aii] + ds_row ]
            row = dict( [ (cd.column, transform_value(val,cd.syapse_cardinality))
                           for cd,val in zip(coldefs,vals) ])
            #if row['workflow_aii'] != u'ivld:LocusWorkflow_19588':
            #    return None
            #import IPython; IPython.embed()
            return row

        ds = self.conn.kb.executeSavedQuery( self.syapse_saved_query_id,
                                             annotate_meta = True)
        for ds_row in ds.rows:
            row = _process_ds_row(self.coldefs,ds_row)
            yield row

    def _build_coldefs(self):
        """builds a list ColDef records"""
        def _make_coldef(h):
            col = camelcase_to_underscore(h.split(':')[-1])
            type = 'TEXT'
            if col.startswith('date_'):
                type = 'TIMESTAMP'
            card = 'ExactlyOne'
            if col.startswith('has_'):
                card = 'Any'
                type += '[]'
            return ColDef(syapse_property = h,
                                     syapse_type = None,
                                     syapse_cardinality = card,
                                     column = col,
                                     type = type,
                                     )
        ds = self.conn.kb.executeSavedQuery( self.syapse_saved_query_id )
        self.coldefs = [ _make_coldef(h)
                         for h in [self.tablename+'_aii'] + ds.headers ]
