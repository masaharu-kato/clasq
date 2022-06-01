"""
    Query data class
"""
from __future__ import annotations
import re
from typing import Collection, Iterable, Iterator, cast

from .abc.query import QueryABC
from .values import NullType, ValueType, is_value_type
from .sql_values import SQLValue
from .exprs import ExprABC, Arg, ArgName, ValueOrArg
from . import errors


class QueryData(QueryABC):
    """ Query procedure data """
    PLACEHOLDER = b'?'
    DEFAULT_SEP = b','
    OBJECT_QUOTE = b'`'
    OBJECT_SEP = b'.'
    RE_KEYWORD = re.compile(rb'[\s\w()+\-*/%<>=!&|^~,.]*')

    def __init__(self,
        *vals: QueryLike | None,
        stmt: bytes | None = None,
        # args: Optional[Collection[Arg]] = None,
        prms: Collection[ValueType | Arg] = None,
    ):
        """ Create a QueryData instance

        Args:
            vals: Keywords, expressions or values for append to the SQL statement.
            stmt (bytes | None, optional): Initial SQL statement bytes. Defaults to None.
            args (Optional[list[Arg]], optional): Initial list of query arguments (placeholders). Defaults to None.
            prms (Optional[list[ValueOrArg]], optional): Initial list of parameter values. Defaults to None.
        """
        self._stmt = stmt if stmt is not None else b''
        # self._argdict = {arg.name: arg for arg in args} if args is not None else {} 
        self._argdict: dict[ArgName, Arg] = {} 
        self._prms = [*prms] if prms else []
        if vals:
            self.append(*vals)

    @property
    def stmt(self) -> bytes:
        """ Get a current statement bytes """
        return self._stmt

    @property
    def args(self) -> tuple[Arg, ...]:
        """ Get a tuple of current query argument objects (placeholders) """
        return tuple(self._argdict.values())

    @property
    def prms_with_args(self) -> tuple[ValueOrArg, ...]:
        """ Get a tuple of current parameter values
            (may includes query argument objects)
        """
        return tuple(self._prms)
    
    @property
    def prms(self) -> tuple[SQLValue, ...]:
        """ Get a tuple of current parameters """
        return self._calc_pure_params()

    def call(self, *argvals: ValueType, **kwargvals: ValueType) -> QueryData:
        """ Create a new QueryData instance with query argument values

        Args:
            argvals: Positional query argument values (Argument which name type is integer)
            kwargvals: Keyword query argument values (Argument which name type is string)

        Raises:
            QueryArgumentError: The specified argument name does not exists.

        Returns:
            QueryData: New QueryData instance with query argument values
        """
        return self._call(argvals, kwargvals)

    def call_ignore_unused(self, *argvals: ValueType, **kwargvals: ValueType) -> QueryData:
        """ Create a new QueryData instance with query argument values
            (Ignore unused arguments)
        """
        return self._call(argvals, kwargvals, ignore_unused=True)

    def calc_prms_many(self, iter_argvals: Iterable[Collection[ValueType] | dict[ArgName, ValueType]], *, ignore_unused=False) -> Iterator[tuple[SQLValue, ...]]:
        for argvals in iter_argvals:
            yield self.calc_prms(argvals, ignore_unused=ignore_unused)

    def calc_prms(self, argvals: Collection[ValueType] | dict[ArgName, ValueType], *, ignore_unused=False) -> tuple[SQLValue, ...]:
        argvaldict = argvals if isinstance(argvals, dict) else dict(enumerate(argvals))
        return self._calc_pure_params(argvaldict, ignore_unused=ignore_unused)

    def __call__(self, *argvals, **kwargvals):
        """ Create a new QueryData instance with query argument values 
            (Synonym of `call` method)
        """
        return self.call(*argvals, **kwargvals)

    def _append(self, stmt: bytes, prms: Iterable[ValueOrArg] = None) -> QueryData:
        """ Append a statement and/or parameters
            (Internal private method)

        Args:
            stmt (bytes): Statement bytes to append
            prms (Iterable[ValueOrArg], optional): Parameters to append. Defaults to None.

        Raises:
            QueryTypeError: Invalid type of parameter value.

        Returns:
            QueryData: Self object
        """
        if stmt:
            if self._stmt \
                and stmt[0:1] not in _R_NOSP_SYMS \
                and self._stmt[-1::1] not in _L_NOSP_SYMS:
                self._stmt += _SPACE
            self._stmt += stmt 
        if prms:
            for prm in prms:
                if not (is_value_type(prm) or isinstance(prm, Arg)):
                    raise errors.QueryTypeError('Invalid parameter value type %s (%s)' % (type(prm), repr(prm)))
                self._prms.append(prm)
        return self

    def append_to_query_data(self, qd: QueryData) -> None:
        """ Append to the existing QueryData
            (Overrided from `QueryABC`)

        Args:
            qd (QueryData): QueryData object to be appended
        """
        qd.append_query_data(self)

    def append_one(self, val: QueryLike | None) -> QueryData:
        """ Append a single value

        Args:
            val (QueryLike | None): A value or object to append.
                `None` will be ignored (do nothing).

        Raises:
            QueryTypeError: Invalid type of value.

        Returns:
            QueryData: Self object
        """

        if val is None:
            return self
        
        if isinstance(val, QueryData):
            return self.append_query_data(val)

        if isinstance(val, QueryABC):
            val.append_to_query_data(self)
            return self

        if isinstance(val, bytes):
            return self.append_keyword(val)

        if is_value_type(val) or isinstance(val, Arg):
        # if isinstance(val, ValueType):
            return self.append_value(cast(ValueOrArg, val))

        if isinstance(val, tuple):
            return self.append(*val)

        if isinstance(val, (set, list)):
            return self.append_joined(val)

        raise errors.QueryTypeError('Invalid value type %s (%s)' % (type(val), repr(val)))

    def append(self, *vals: QueryLike | None) -> QueryData:
        """ Append values

        Args:
            *vals (QueryLike | None): Value(s) or object(s) to append.
                `None` will be ignored (do nothing).

        Raises:
            ObjectArgTypeError: Invalid type of value.

        Returns:
            QueryData: Self object
        """
        for val in vals:
            self.append_one(val)
        return self

    def append_keyword(self, keyword: bytes) -> QueryData:
        """ Append SQL keyword or raw statements
            (Quotes and multiple statements are not allowed.)

        Args:
            keyword (bytes): Keyword bytes to append

        Raises:
            errors.QueryValueError: Keyword has invalid character(s).

        Returns:
            QueryData: Self object
        """
        if not (isinstance(keyword, bytes) and self.RE_KEYWORD.fullmatch(keyword)):
            raise errors.QueryValueError('Keyword has invalid characters.', keyword)
        return self._append(keyword)

    def append_query_data(self, qd: QueryData) -> QueryData:
        """ Append the other QueryData object 

        Args:
            qd (QueryData): QueryData to append

        Returns:
            QueryData: Self object
        """
        return self._append(qd._stmt, qd._prms)

    def append_value(self, val: ValueOrArg) -> QueryData:
        """ Append as value

        Args:
            val (ValueOrArg): Value or query argument object to append

        Raises:
            QueryArgumentError: Different arguments with same name are specified.

        Returns:
            QueryData: Self object
        """
        if isinstance(val, Arg):
            if val.name in self._argdict:
                if not self._argdict[val.name].is_same_arg(val):
                    raise errors.QueryArgumentError('Cannot specify different arguments with same name.', val.name)
            else:
                self._argdict[val.name] = val
        return self._append(self.PLACEHOLDER, [val])

    def append_values(self, *vals: ValueOrArg) -> QueryData:
        """ Append multiple args as values

        Args:
            *vals (ValueOrArg): Value(s) or query argument object(s) to append

        Returns:
            QueryData: Self object
        """
        for val in vals:
            self.append_value(val)
        return self

    def append_object_name(self, val: bytes) -> QueryData:
        """ Append as object name

        Args:
            val (bytes): Object name to append

        Returns:
            QueryData: Self object
        """
        assert isinstance(val, bytes) and not self.OBJECT_QUOTE in val
        return self._append(self.OBJECT_QUOTE + val + self.OBJECT_QUOTE)
        
    def append_joined_object(self, *vals: QueryLike | None) -> QueryData:
        """ Join values with a default separator (, ) and append

        Returns:
            QueryData: Self object
        """
        return self.append_joined(vals, sep=self.OBJECT_SEP)

    def __iadd__(self, value) -> QueryData:
        """ Append a single value
            (Synonym of `append_one` method)
        """
        return self.append_one(value)

    def append_joined(self, vals: Iterable, *, sep:bytes = DEFAULT_SEP) -> QueryData:
        """ Join a iterable with a specific separator

        Args:
            vals (Iterable): Iterable to join
            sep (bytes, optional): Separator. Defaults to DEFAULT_SEP (, ).

        Returns:
            QueryData: Self object
        """
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
    

    def _call(self, argvals: tuple[ValueType, ...], kwargvals: dict[str, ValueType], *, ignore_unused=False) -> QueryData:
        argvaldict: dict[ArgName, ValueType] = {}
        for i, argval in enumerate(argvals):
            if i not in self._argdict:
                raise errors.QueryArgumentError('Positional argument %s not found.' % i)
            argvaldict[i] = argval
        
        for argname, argval in kwargvals.items():
            if argname not in self._argdict:
                raise errors.QueryArgumentError('Keyword argument `%s` not found.' % argname)
            argvaldict[argname] = argval

        return QueryData(stmt=self._stmt, prms=self._calc_params_with_args(argvaldict, ignore_unused=ignore_unused))

    def _calc_params_with_args(self, argvaldict: dict[ArgName, ValueType] | None, *, ignore_unused=False):
        # nondefault_args = [arg for arg in self._argdict.values() if not arg.has_default]
        # if nondefault_args:
        #     raise errors.QueryArgumentError('Argument value(s) are not set: %s' % ', '.join(str(arg) for arg in nondefault_args))

        unused_argnames: set[ArgName] = {arg for arg in argvaldict} if argvaldict is not None else set()
        new_prms: list[ValueType | Arg] = []

        for prm in self._prms:

            if argvaldict is not None and isinstance(prm, Arg) and prm.name in argvaldict:
                prmval: ValueType | Arg = argvaldict[prm.name]
                unused_argnames.remove(prm.name)
            else:
                prmval = prm

            new_prms.append(prmval)

        if not ignore_unused and unused_argnames:
            raise errors.QueryArgumentError('Unused arguments exist: %s' % ', '.join(str(name) for name in unused_argnames))

        return new_prms


    def _calc_pure_params(self, argvaldict: dict[ArgName, ValueType] | None = None, *, ignore_unused=False):
        # nondefault_args = [arg for arg in self._argdict.values() if not arg.has_default]
        # if nondefault_args:
        #     raise errors.QueryArgumentError('Argument value(s) are not set: %s' % ', '.join(str(arg) for arg in nondefault_args))

        unused_argnames: set[ArgName] = {arg for arg in argvaldict} if argvaldict is not None else set()
        unset_args: list[Arg] = []
        new_prms: list[SQLValue] = []

        for prm in self._prms:

            if argvaldict is not None and isinstance(prm, Arg) and prm.name in argvaldict:
                prmval: ValueType | Arg = argvaldict[prm.name]
                unused_argnames.remove(prm.name)
            elif isinstance(prm, Arg) and prm.has_default:
                prmval = prm.default
            else:
                prmval = prm

            if isinstance(prmval, Arg):
                unset_args.append(prmval)
            else:
                sqlval: SQLValue = None if isinstance(prmval, NullType) else prmval
                new_prms.append(sqlval)

        if unset_args:
            raise errors.QueryArgumentError('Argument value(s) are not set: %s' % ', '.join(str(arg.name) for arg in unset_args))
        if not ignore_unused and unused_argnames:
            raise errors.QueryArgumentError('Unused arguments exist: %s' % ', '.join(str(name) for name in unused_argnames))

        return tuple(new_prms)


QueryLike = ValueOrArg | ExprABC | QueryABC | tuple | Iterable

QueryArgVals = Collection[ValueType] | dict[ArgName, ValueType]


_R_NOSP_SYMS = {b' ', b')', b',', b'.'}
_L_NOSP_SYMS = {b' ', b'(', b'.'}
_SPACE = b' '
