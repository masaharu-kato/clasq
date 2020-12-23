
_IS_DEBUG = True

def _fo(objname:str) -> str:
    """ Format single object name """
    if _IS_DEBUG:
        if '`' in objname:
            raise RuntimeError('Invalid character(s) found in the object name.')
    return '`' + objname + '`'


def _fmo(objname:str) -> str:
    """ Format multi-leveled object name """
    return '.'.join(map(_fo, objname.split('.')))


def _type(typename:str) -> str:
    if _IS_DEBUG:
        if not re.match(r'\w+(\(\w*\))?', typename):
            raise RuntimeError('Invalid typename "{}".'.format(typename))
    return typename


def _str(text:str) -> str:
    return "'" + text.replace("'", "''") + "'"
