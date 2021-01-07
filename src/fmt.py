""" 
    SQL text format module
"""
import re

_IS_DEBUG = True

JOINTYPES = ['INNER', 'RIGHT', 'LEFT']
ORDERTYPES = ['ASC', 'DESC']


def fo(_objname) -> str:
    """ Format single object name """
    objname = str(_objname)
    if _IS_DEBUG:
        if '`' in objname:
            raise RuntimeError('Invalid character(s) found in the object name.')
    return '`' + objname + '`'


def fmo(objname:str) -> str:
    """ Format multi-leveled object name """
    return '.'.join(map(fo, objname.split('.')))


def sqltype(typename:str) -> str:
    """ Format SQL type """
    if _IS_DEBUG:
        if not re.match(r'\w+(\(\w*\))?', typename):
            raise RuntimeError('Invalid typename "{}".'.format(typename))
    return typename


def strval(text:str) -> str:
    """ Format string as value """
    return "'" + str(text).replace("'", "''") + "'"


def jointype(text:str) -> str:
    """ Format join type """
    if not str(text).upper() in JOINTYPES:
        raise RuntimeError('Invalid jointype.')
    return text
    

def ordertype(text:str) -> str:
    """ Format order type """
    if not str(text).upper() in ORDERTYPES:
        raise RuntimeError('Invalid jointype.')
    return text
