import syapse_client

from multicorn.utils import log_to_postgres

import ConfigParser, collections, re

def pg_log(msg):
    log_to_postgres('syapsefdw: '+msg)

def map_values(vals):
    def _map_value(v):
        """transform values in triples items to unicode strings"""
        if isinstance(v,syapse_client.lobj.User):
            return v.email
        if isinstance(v,syapse_client.lobj.Project):
            return v.short_name
        return v
    return [ _map_value(v) for v in vals ]

def transform_value(v,cardinality):
    # v may be a list or scalar, and cardinality may be any
    # if cardinality is 'Any', return set
    if cardinality == 'Any':
        return v if isinstance(v,list) else [v]
    # else cardinality is 'ExactlyOne' or 'AtMostOne', return None or value
    if not isinstance(v,list):
        return v
    return v[0] if len(v)>0 else None

def camelcase_to_underscore(t):
    """e.g., camelCase -> camel_case, SyapseFDW -> syapse_fdw"""
    return re.sub(r'([a-z])([A-Z]+)(?=[a-z]*)', r'\1_\2', t).lower()

def savedquery_to_tablename(t):
    """fdw:Blood -> fdw_blood"""
    if t.startswith('fdw:'):
        t = t[4:]
    return camelcase_to_underscore(t)
