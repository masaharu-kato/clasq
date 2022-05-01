"""
    Query data class
"""
from enum import Enum
from typing import Iterable, Optional, Union

from .expr_abc import ExprABC


class QueryData:
    """ Query procedure data """
    PLACEHOLDER = b'?'
    DEFAULT_SEP = b', '

    def __init__(self, *vals, prms: Optional[list] = None):
        self._stmt = b''
        self._prms = prms if prms else []
        self.append(*vals)

    @property
    def stmt(self):
        return self._stmt

    @property
    def prms(self):
        return self._prms

    def iter_prms(self):
        return iter(self._prms)

    def _append(self, stmt: bytes, prms: Optional[list] = None) -> 'QueryData':
        if stmt:
            if stmt[0:1] not in _NOSPACE_SYMBOLS:
                self._stmt += _SPACE
            self._stmt += stmt 
        if prms:
            for prm in prms:
                if not isinstance(prm, (bool, int, float, str)):
                    raise TypeError('Invalid parameter value type %s (%s)' % (type(prm), repr(prm)))
            self._prms += prms
        return self

    def _append_qd(self, qd: 'QueryData') -> 'QueryData':
        return self._append(qd._stmt, qd._prms)

    def append_as_value(self, val) -> 'QueryData':
        return self._append(self.PLACEHOLDER, [val])

    def append_one(self, val, prms: Optional[list] = None) -> 'QueryData':

        print(type(val), val, ', prms=', prms)

        if val is None:
            assert prms is None
            return self
        
        if isinstance(val, QueryData):
            assert prms is None
            return self._append_qd(val)

        if isinstance(val, ExprABC):
            assert prms is None
            val.append_query_data(self)
            return self

        if isinstance(val, Enum):
            return self.append_one(val.value)

        if isinstance(val, bytes):
            return self._append(val, prms)

        if isinstance(val, tuple):
            return self.append(*val)

        if isinstance(val, Iterable):
            return self.append_joined(val)

        if isinstance(val, (bool, int, float, str)):
            return self.append_as_value(val)

        raise TypeError('Invalid value type %s (%s)' % (type(val), repr(val)))


    def append(self, *vals) -> 'QueryData':
        for val in vals:
            self.append_one(val)
        return self

    def __iadd__(self, value) -> 'QueryData':
        return self.append_one(value)

    def append_joined(self, vals: Iterable, *, sep:bytes = DEFAULT_SEP) -> 'QueryData':
        for i, val in enumerate(vals):
            if i > 0:
                self.append_one(sep)
            self.append_one(val)
        return self

    def __repr__(self) -> str:
        return 'QueryData("%r", [%s]' % (self._stmt, ', '.join(map(repr, self._prms)))


_NOSPACE_SYMBOLS = {b'(', b')', b',', b'.'}
_SPACE = b' '
