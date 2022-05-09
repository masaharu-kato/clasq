"""
    Query data class
"""
from typing import TYPE_CHECKING, Dict, Iterable, Iterator, List, Optional, Tuple, Union, cast

from .query_abc import QueryABC
from .values import NullType, ValueType, is_value_type
from .sql_values import SQLValue
from .exprs import ExprABC, Arg, ValueOrArg


class QueryArgumentError(RuntimeError):
    """ Query Argument Error """


class QueryData(QueryABC):
    """ Query procedure data """
    PLACEHOLDER = b'?'
    DEFAULT_SEP = b','
    OBJECT_QUOTE = b'`'
    OBJECT_SEP = b'.'

    def __init__(self,
        *vals,
        stmt: Optional[bytes] = None,
        args: Optional[List[Arg]] = None,
        prms: Optional[List[ValueOrArg]] = None,
    ):
        self._stmt = stmt if stmt is not None else b''
        self._argdict = {arg.name: arg for arg in args} if args is not None else {}
        self._prms = prms if prms else []
        if vals:
            self.append(*vals)

    @property
    def stmt(self) -> bytes:
        return self._stmt

    @property
    def args(self) -> Tuple[Arg, ...]:
        return tuple(self._argdict.values())

    @property
    def prms_with_args(self) -> Tuple[ValueOrArg, ...]:
        return tuple(self._prms)

    def iter_prms(self) -> Iterator[SQLValue]:
        nondefault_args = [arg for arg in self._argdict.values() if not arg.has_default]
        if nondefault_args:
            raise QueryArgumentError('Argument value(s) are not set: %s' % ', '.join(str(arg) for arg in nondefault_args))
        for prm in self._prms:
            prmval = prm.default if isinstance(prm, Arg) else prm
            yield None if isinstance(prmval, NullType) else prmval
    
    @property
    def prms(self) -> Tuple[SQLValue, ...]:
        return (*self.iter_prms(),)

    def call(self, *argvals: ValueType, **kwargvals: ValueType) -> 'QueryData':
        all_argvals: Dict[Union[int, str], ValueType] = {}

        for i, argval in enumerate(argvals):
            if i not in self._argdict:
                raise QueryArgumentError('Positional argument %s not found.' % i)
            all_argvals[i] = argval
        
        for argname, argval in kwargvals.items():
            if argname not in self._argdict:
                raise QueryArgumentError('Keyword argument `%s` not found.' % argname)
            all_argvals[argname] = argval

        unset_args = [arg for arg in self._argdict.values() if arg.name not in all_argvals]

        new_prms = [
            all_argvals[prm.name] if isinstance(prm, Arg) and prm.name in all_argvals else prm
            for prm in self._prms
        ]

        # print('all_argvals, unset_args, new_prms=', all_argvals, unset_args, new_prms)

        return QueryData(stmt=self._stmt, args=unset_args, prms=new_prms)

    def __call__(self, *argvals, **kwargvals):
        return self.call(*argvals, **kwargvals)

    def _append(self, stmt: bytes, prms: Iterable[ValueOrArg] = None) -> 'QueryData':
        if stmt:
            if self._stmt \
                and stmt[0:1] not in _R_NOSP_SYMS \
                and self._stmt[-1::1] not in _L_NOSP_SYMS:
                self._stmt += _SPACE
            self._stmt += stmt 
        if prms:
            for prm in prms:
                if not (is_value_type(prm) or isinstance(prm, Arg)):
                    raise TypeError('Invalid parameter value type %s (%s)' % (type(prm), repr(prm)))
                self._prms.append(prm)
        return self

    def _append_qd(self, qd: 'QueryData') -> 'QueryData':
        return self._append(qd._stmt, qd._prms)

    def append_query_data(self, qd: 'QueryData') -> None:
        qd._append_qd(self)

    def append_value(self, val: ValueOrArg) -> 'QueryData':
        """ Append as value

        Args:
            val (ValueOrArg): value to append

        Returns:
            QueryData: This object
        """
        if isinstance(val, Arg):
            if val.name in self._argdict:
                if not self._argdict[val.name].is_same_arg(val):
                    raise QueryArgumentError('Cannot specify different arguments with same name.', val.name)
            else:
                self._argdict[val.name] = val
        return self._append(self.PLACEHOLDER, [val])

    def append_values(self, *vals: ValueOrArg) -> 'QueryData':
        """ Append multiple args as values

        Returns:
            QueryData: This object
        """
        for val in vals:
            self.append_value(val)
        return self

    def append_object(self, val: bytes) -> 'QueryData':
        assert isinstance(val, bytes) and not self.OBJECT_QUOTE in val
        return self._append(self.OBJECT_QUOTE + val + self.OBJECT_QUOTE)
        
    def append_joined_object(self, *vals: Optional['QueryLike']) -> 'QueryData':
        return self.append_joined(vals, sep=self.OBJECT_SEP)

    def append_one(self, val: Optional['QueryLike'], prms: Optional[list] = None) -> 'QueryData':

        # print(type(val), val, ', prms=', prms)

        if val is None:
            assert prms is None
            return self
        
        if isinstance(val, QueryData):
            assert prms is None
            return self._append_qd(val)

        if isinstance(val, QueryABC):
            assert prms is None
            val.append_query_data(self)
            return self

        if isinstance(val, bytes):
            return self._append(val, prms)

        if is_value_type(val) or isinstance(val, Arg):
        # if isinstance(val, ValueType):
            return self.append_value(cast(ValueOrArg, val))

        if isinstance(val, tuple):
            return self.append(*val)

        if isinstance(val, Iterable):
            return self.append_joined(val)

        raise TypeError('Invalid value type %s (%s)' % (type(val), repr(val)))


    def append(self, *vals: Optional['QueryLike']) -> 'QueryData':
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

QueryLike = Union[ValueOrArg, ExprABC, QueryABC, tuple, Iterable]


_R_NOSP_SYMS = {b' ', b')', b',', b'.'}
_L_NOSP_SYMS = {b' ', b'(', b'.'}
_SPACE = b' '
