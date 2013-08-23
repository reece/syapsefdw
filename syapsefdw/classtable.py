import syapse_client

from syapsefdw import SyapseFDW
from utils import map_values, transform_value

class ClassTable(SyapseFDW):
    """Represents a Syapse class-based data source.  The current Syapse
    API has no filtering or bulk fetch mechanism.  Therefore, records are
    returned by fetching them one-by-one.  This interface is extremely
    slow -- consider making a saved query instead."""

    def __init__(self,options,columns):
        super(ClassTable, self).__init__(options,columns)
        self.syapse_class = options['source']
        self.tablename = camelcase_to_underscore(self.syapse_class)
        self._build_coldefs()

    def execute(self, quals, columns):
        airs = self.conn.kb.listAppIndividualRecords(kb_class_id=self.syapse_class)
        for air in airs:
            ai = self.conn.kb.retrieveAppIndividual(air.app_ind_id)
            triples_items = ai.triplesItems(full=True)
            # BROKEN HERE: adapt to coldefs list rather than coldefs dict
            row = dict([ (self.coldefs[k].column, _transform_value(self.coldefs[k],_map_values(vals)))
                         for k,vals in triples_items ])
            yield row

    def _build_coldefs(self):
        """builds a dict of syapse property name => ColDef record"""
        def _syapse_type(ps):
            return ps.prop.range if isinstance(ps.prop.range,unicode) else None
        def _make_coldef(ps):
            return ColDef(syapse_property = ps.prop.id, 
                          syapse_type = _syapse_type(ps),
                          syapse_cardinality = ps.cardinality,
                          column = camelcase_to_underscore(ps.prop.id),
                          type = 'TEXT',
                        )
        self.coldefs = [ _make_coldef(ps)
                         for ps in self.conn.kb.getForm(self.syapse_class).props.values() ]
