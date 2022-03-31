"""
    SQL Syntax Format methods
"""

import re

from . import keywords

_IS_DEBUG = True

class InvalidFormatError(ValueError):
    """ Invalid Format error """


def asobj(_objname) -> str:
    """ Format single object name """
    if _objname is None:
        return ''
    name = str(_objname)
    if _IS_DEBUG:
        if '`' in name:
            raise InvalidFormatError('Invalid character(s) found in the object name: %s' % name)
    return '`' + name + '`'

def joinobjs(*objs) -> str:
    return '.'.join(objs)

def joinasobjs(*objs) -> str:
    return joinobjs(*map(asobj, objs))

def asmultiobj(_objname) -> str:
    return joinasobjs(str(_objname).split('.'))

def astext(_text) -> str:
    """ Format single object name """
    name = '' if _text is None else str(_text)
    if _IS_DEBUG:
        if '"' in name:
            raise InvalidFormatError('Invalid character(s) found in the text.')
    return '"' + name + '"'

def astype(typename) -> str:
    """ Format SQL type """
    if _IS_DEBUG:
        if not re.match(r'\w+(\(\w*\))?', typename):
            raise InvalidFormatError('Invalid typename "{}".'.format(typename))
    return typename

def asop(opname) -> str:
    op = str(opname).upper()
    if op in keywords.OP_ALIASES:
        op = keywords.OP_ALIASES[op]
    if op not in keywords.OPS:
        raise InvalidFormatError('Invalid operator `%s`' % op)
    return op

