"""
    Query data class
"""
from enum import Enum
from typing import Iterable, Optional, Union

from .expr_abc import ExprABCBase
from .values import NULL, is_value_type


class QueryData:
    """ Query procedure data """
    PLACEHOLDER = b'?'
    DEFAULT_SEP = b','
    OBJECT_QUOTE = b'`'
    OBJECT_SEP = b'.'

    def __init__(self,
        *vals,
        prms: Optional[list] = None,
    ):
        self._stmt = b''
        self._prms = prms if prms else []
        if vals:
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
            if self._stmt \
                and stmt[0:1] not in _R_NOSP_SYMS \
                and self._stmt[-1::1] not in _L_NOSP_SYMS:
                self._stmt += _SPACE
            self._stmt += stmt 
        if prms:
            for prm in prms:
                if not is_value_type(prm):
                    raise TypeError('Invalid parameter value type %s (%s)' % (type(prm), repr(prm)))
                self._prms.append(None if prm == NULL else prm)
        return self

    def _append_qd(self, qd: 'QueryData') -> 'QueryData':
        return self._append(qd._stmt, qd._prms)

    def append_value(self, val) -> 'QueryData':
        return self._append(self.PLACEHOLDER, [val])

    def append_values(self, *vals) -> 'QueryData':
        for val in vals:
            self.append_value(val)
        return self

    def append_object(self, val: bytes) -> 'QueryData':
        assert isinstance(val, bytes) and not self.OBJECT_QUOTE in val
        return self._append(self.OBJECT_QUOTE + val + self.OBJECT_QUOTE)
        
    def append_joined_object(self, *vals) -> 'QueryData':
        return self.append_joined(vals, sep=self.OBJECT_SEP)

    def append_one(self, val, prms: Optional[list] = None) -> 'QueryData':

        # print(type(val), val, ', prms=', prms)

        if val is None:
            assert prms is None
            return self
        
        if isinstance(val, QueryData):
            assert prms is None
            return self._append_qd(val)

        if isinstance(val, ExprABCBase):
            assert prms is None
            val.append_query_data(self)
            return self

        if isinstance(val, bytes):
            return self._append(val, prms)

        if is_value_type(val):
            return self.append_value(val)

        if isinstance(val, Enum):
            return self.append_one(val.value)

        if isinstance(val, tuple):
            return self.append(*val)

        if isinstance(val, Iterable):
            return self.append_joined(val)

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

    def __eq__(self, value) -> bool:
        if not isinstance(value, QueryData):
            return NotImplemented
        return self._stmt == value._stmt and self._prms == value._prms

    def __repr__(self) -> str:
        return 'QueryData(%s, [%s])' % (self._stmt.decode(), ', '.join(map(repr, self._prms)))


_R_NOSP_SYMS = {b' ', b')', b',', b'.'}
_L_NOSP_SYMS = {b' ', b'(', b'.'}
_SPACE = b' '
