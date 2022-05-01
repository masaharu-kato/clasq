"""
    Query data class
"""
from typing import Iterable, Optional, Union

from .expr_abc import ExprABC


class QueryData:
    """ Query procedure data """
    PLACEHOLDER = b'?'
    DEFAULT_SEP = b', '

    def __init__(self, stmt: Optional[bytes] = None, prms: Optional[list] = None):
        
        self._stmt = stmt if stmt else b''
        self._prms = prms if prms else []

    @property
    def stmt(self):
        return self._stmt

    @property
    def prms(self):
        return self._prms

    def iter_prms(self):
        return iter(self._prms)

    def _append(self, stmt: bytes, prms: Optional[list] = None) -> 'QueryData':
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

    def append(self, val, prms: Optional[list] = None) -> 'QueryData':

        print(type(val), val, ', prms=', prms)
        
        if isinstance(val, QueryData):
            assert prms is None
            return self._append_qd(val)

        if isinstance(val, ExprABC):
            assert prms is None
            val.append_query_data(self)
            return self

        if isinstance(val, bytes):
            return self._append(val, prms)

        if isinstance(val, Iterable):
            return self.append_joined(val)

        if isinstance(val, (bool, int, float, str)):
            return self.append_as_value(val)

        raise TypeError('Invalid value type %s (%s)' % (type(val), repr(val)))


    def extend(self, *vals) -> 'QueryData':
        for val in vals:
            self.append(val)
        return self

    def __iadd__(self, value) -> 'QueryData':
        return self.append(value)

    def append_joined(self, vals: Iterable, *, sep:bytes = DEFAULT_SEP) -> 'QueryData':
        for i, val in enumerate(vals):
            if i > 0:
                self.append(sep)
            self.append(val)
        return self

    def __repr__(self) -> str:
        return 'QueryData("%r", [%s]' % (self._stmt, ', '.join(map(repr, self._prms)))
