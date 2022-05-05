"""
    View classes
"""
from abc import abstractmethod
from typing import TYPE_CHECKING, Dict, Iterable, Iterator, List, Optional, Tuple, Type, Union

from ..syntax.keywords import JoinType, JoinLike, OrderLike
from ..syntax.exprs import ExprABC, OP, NamedExpr, ObjectABC, Object, NamedExprABC, Name, to_name
from ..syntax import errors
from ..utils.tabledata import TableData

if TYPE_CHECKING:
    from ..syntax.query_data import QueryData
    from .table import Table
    from .database import Database


class ViewABC(Object):
    """ View Expr """

    def __init__(self,
        name: Name,
        *_nexprs: Optional[NamedExprABC],
        database: Optional['Database'] = None,
        exists_on_db: bool = False,
        dynamic: bool = False,
    ):
        super().__init__(name)
        assert database is None or type(database).__name__ == 'Database' 
        self._database = database
        self._exists_on_db = exists_on_db
        self._dynamic = dynamic

        nexprs = [c for c in _nexprs if c is not None]
        self._nexpr_dict: Dict[bytes, NamedExprABC] = {}
        for nexpr in nexprs:
            self.append_named_expr(nexpr)
        # self._options = options
            
        self._query_data : Optional['QueryData'] = None # SELECT query data
        self._result : Optional[TableData] = None # View Result

    def __repr__(self):
        return 'View(%s)' % str(self)

    @property
    def database(self):
        if self._database is None:
            raise errors.ObjectNotSetError('Database is not set.')
        return self._database

    @property
    def db(self):
        return self.database

    @property
    def database_or_none(self):
        return self._database

    @property
    def cnx(self):
        return self.database.cnx

    @property
    def exists_on_db(self):
        return self._exists_on_db

    @property
    def is_dynamic(self):
        return self._dynamic

    # @property
    # def options(self):
    #     return self._options

    def iter_named_exprs(self) -> Iterator[ObjectABC]:
        return (c for c in self._nexpr_dict.values())

    def set_database(self, database: 'Database') -> None:
        """ Set a Database object """
        if self._database is not None:
            raise errors.ObjectAlreadySetError('Database already set.')
        self._database = database

    def column(self, val: Union[Name, NamedExprABC]) -> NamedExprABC:
        """ Get a Column object with the specified name
                or check existing Column object is valid for this Table object

        Args:
            val (bytes | str | ObjectABC): Column name or Column object

        Raises:
            errors.ObjectNotFoundError: _description_
            errors.NotaSelfObjectError: _description_
            errors.ObjectArgTypeError: _description_

        Returns:
            Column: Column object with the specified name or Column object itself
        """
        if isinstance(val, (bytes, str)):
            name = to_name(val)
            if name not in self._nexpr_dict:
                if not self.is_dynamic:
                    raise errors.ObjectNotFoundError('Undefined column name `%r` on view `%r`' % (name, self._name))
                self._nexpr_dict[name] = NamedExpr(name)
            return self._nexpr_dict[name]
            
        if isinstance(val, NamedExprABC):
            if not val in self._nexpr_dict.values():
                if not self.is_dynamic:
                    raise errors.NotaSelfObjectError('Not a column of this view.')
                self._nexpr_dict[val.name] = val
            return val

        raise errors.ObjectArgTypeError('Invalid type %s (%s)' % (type(val), val))

    def col(self, val: Union[Name, NamedExprABC]):
        """ Synonym of `column` method """
        return self.column(val)

    def column_or_none(self, val: Union[Name, NamedExprABC]) -> Optional[NamedExprABC]:
        """ Get a Column object with the specified name if exists """
        try:
            return self.column(val)
        except (errors.ObjectNotFoundError, errors.NotaSelfObjectError):
            pass
        return None

    def get(self, val: Union[Name, NamedExprABC]) -> Optional[NamedExprABC]:
        """ Synonym of `column_or_none` method """
        return self.column_or_none(val)

    def append_named_expr(self, nexpr: NamedExprABC) -> None:
        """ Append (existing) named expression object

        Args:
            nexpr (NamedExprABC): Named expression object
        """
        self._nexpr_dict[nexpr.name] = nexpr

    def get_froms(self) -> Optional[Iterable[Union['View', Name]]]:
        return None # Default Implementation

    def get_joins(self) -> Optional[Iterable[Tuple[Union['View', Name], JoinLike, Optional[ExprABC]]]]:
        return None # Default Implementation

    def get_where(self) -> Optional[ExprABC]:
        return None # Default Implementation

    def get_groups(self) -> Optional[Iterable[NamedExprABC]]:
        return None # Default Implementation

    def get_orders(self) -> Optional[Iterable[NamedExprABC]]:
        return None # Default Implementation

    def get_limit(self) -> Optional[int]:
        return None # Default Implementation

    def get_offset(self) -> Optional[int]:
        return None # Default Implementation

    def clone(self,
        *_columns: Optional[ObjectABC], 
        database : Optional['Database'] = None,
        name     : Optional[Name] = None,
        froms    : Optional[Iterable[Union['ViewABC', Name]]] = None,
        joins    : Optional[Iterable[Tuple[Union['ViewABC', Name], JoinLike, Optional[ExprABC]]]] = None,
        where    : Optional[ExprABC] = None,
        groups   : Optional[Iterable[NamedExprABC]] = None,
        orders   : Optional[Iterable[NamedExprABC]] = None,
        limit    : Optional[int] = None,
        offset   : Optional[int] = None,
    ) -> 'ViewABC':

        return self._new_view(
            *self.iter_named_exprs(), *_columns,
            database = database if database is not None else self._database,
            name = name if name is not None else self._name,
            froms = self._join_as_tuple(self.get_froms(), froms),
            joins = self._join_as_tuple(self.get_joins(), joins),
            where = OP.AND.call_joined_opt(self.get_where(), where),
            groups = self._join_as_tuple(self.get_groups(), groups),
            orders = self._join_as_tuple(self.get_orders(), orders),
            limit = limit if limit is not None else self.get_limit(),
            offset = offset if offset is not None else self.get_offset(),
        )

    def merged(self, view: 'View') -> 'ViewABC':
        return self.clone(
            *view.iter_named_exprs(),
            database = view._database,
            name   = view._name,
            froms  = view.get_froms(),
            joins  = view.get_joins(),
            where  = view.get_where(),
            groups = view.get_groups(),
            orders = view.get_orders(),
            limit  = view.get_limit(),
            offset = view.get_offset(),
        )

    def select_column(self, *_columns: Optional[ObjectABC]) -> 'ViewABC':
        """ Make a View object with columns """
        return self.clone(*_columns)

    def where(self, *exprs, **coleqs) -> 'ViewABC':
        """ Make a View object with WHERE clause """
        return self.clone(where=self._proc_terms(*exprs, **coleqs))

    def group_by(self, *columns: NamedExprABC, **cols: Optional[bool]) -> 'ViewABC':
        return self.clone(groups=[
            *columns,
            *(self.column(c) for c, v in cols.items() if v)
        ])

    def order_by(self, *orders: NamedExprABC, **col_orders: Optional[OrderLike]) -> 'ViewABC':
        """ Make a View object with ORDER BY clause """
        return self.clone(orders=[
            *orders,
            *(self.column(c).ordered(v) for c, v in col_orders.items() if v is not None)
        ])

    def limit(self, limit: int) -> 'ViewABC':
        """ Make a View object with LIMIT OFFSET clause """
        return self.clone(limit=limit)

    def offset(self, offset: int) -> 'ViewABC':
        """ Make a View object with LIMIT OFFSET clause """
        return self.clone(offset=offset)

    # def single(self) -> 'SingleView':
    #     """ Make a View object with a single result """
    #     return SingleView(self)

    def join(self, join_type: JoinLike, view: Union['ViewABC', Name], *exprs: ExprABC, **_coleqs: Union[Name, NamedExprABC]) -> 'ViewABC':
        """ Make a Joined View """
        coleqs = {n: self.column(v) for n, v in _coleqs.items()}
        return self.clone(joins=[(
            self._proc_view(view),
            JoinType.make(join_type),
            self._proc_terms(*exprs, **coleqs)
        )])

    def inner_join(self, view: Union['ViewABC', Name], *exprs: ExprABC, **coleqs: Union[Name, NamedExprABC]) -> 'ViewABC':
        """ Make a INNER Joined View """
        return self.join(JoinType.INNER, view, *exprs, **coleqs)

    def left_join(self, view: Union['ViewABC', Name], *exprs: ExprABC, **coleqs: Union[Name, NamedExprABC]) -> 'ViewABC':
        """ Make a LEFT Joined View """
        return self.join(JoinType.LEFT, view, *exprs, **coleqs)

    def right_join(self, view: Union['ViewABC', Name], *exprs: ExprABC, **coleqs: Union[Name, NamedExprABC]) -> 'ViewABC':
        """ Make a RIGHT Joined View """
        return self.join(JoinType.RIGHT, view, *exprs, **coleqs)

    def outer_join(self, view: Union['ViewABC', Name], *exprs: ExprABC, **coleqs: Union[Name, NamedExprABC]) -> 'ViewABC':
        """ Make a OUTER Joined View """
        return self.join(JoinType.OUTER, view, *exprs, **coleqs)

    def cross_join(self, view: Union['ViewABC', Name], *exprs: ExprABC, **coleqs: Union[Name, NamedExprABC]) -> 'ViewABC':
        """ Make a CROSS Joined View """
        return self.join(JoinType.CROSS, view, *exprs, **coleqs)

    def __getitem__(self, val):

        if isinstance(val, int):
            return self.clone(offset=val, limit=1)

        if isinstance(val, slice):
            assert not val.step # TODO: Implementation
            if val.start:
                if val.stop:
                    return self.clone(offset=val.start, limit=(val.stop-val.start))
                return self.clone(offset=val.start)
            else:
                if val.stop:
                    return self.clone(limit=val.stop)
            return self

        if isinstance(val, (bytes, str)):
            return self.column(val)
        
        if isinstance(val, ExprABC):
            return self.clone(where=val)

        raise TypeError('Invalid type %s (%s)' % (type(val), val))


    def refresh_query_data(self) -> None:
        """ Refresh QueryData """
        self._query_data = self.db.select_query(
            *self.iter_named_exprs(),
            froms  = self.get_froms(),
            joins  = self.get_joins(),
            where  = self.get_where(),
            groups = self.get_groups(),
            orders = self.get_orders(),
            limit  = self.get_limit(),
            offset = self.get_offset(),
        )

    def prepare_query_data(self) -> None:
        """ Prepare QueryData """
        if self._query_data is None:
            self.refresh_query_data()
    
    @property
    def is_query_data_ready(self) -> bool:
        return self._query_data is not None

    @property
    def query_data(self) -> 'QueryData':
        self.prepare_query_data()
        assert self._query_data is not None
        return self._query_data
    
    def refresh_result(self) -> None:
        self._result = self.db.query_qd(self.query_data)

    def prepare_result(self) -> None:
        if self._result is None:
            return self.refresh_result()

    @property
    def is_result_ready(self) -> bool:
        return self._result is not None

    @property
    def result(self):
        self.prepare_result()
        assert self._result is not None
        return self._result

    def __iter__(self):
        return iter(self.result)

    def __eq__(self, value):
        if isinstance(value, TableData):
            return self.result == value
        return super().__eq__(value)

    def check_eq(self, value: 'ViewABC') -> bool:
        if not isinstance(value, ViewABC):
            raise TypeError('Invalid value type.')
        view = value
        return [*self.iter_named_exprs()] == [*view.iter_named_exprs()] \
            and self._database == view._database \
            and self._name == view._name \
            and self.get_froms()  == view.get_froms() \
            and self.get_joins()  == view.get_joins() \
            and self.get_where()  == view.get_where() \
            and self.get_groups() == view.get_groups() \
            and self.get_orders() == view.get_orders() \
            and self.get_limit()  == view.get_limit() \
            and self.get_offset() == view.get_offset()
    
    def __hash__(self) -> int:
        return super().__hash__()

    def drop(self, *, if_exists=False):
        """ Run DROP VIEW query """
        if_exists = if_exists or (not self._exists_on_db)
        return self.db.execute(
            b'DROP', b'VIEW',
            b'IF NOT EXISTS' if if_exists else None, self)

    def create(self, *, if_not_exists=False, drop_if_exists=False) -> None:
        """ Create this View on the database """
        # if drop_if_exists:
        #     self.drop(if_exists=True)
        # self.db.execute(
        #     b'CREATE', b'VIEW',
        #     b'IF NOT EXISTS' if if_not_exists else None,
        #     self, b'(', [c.q_create() for c in self.iter_named_exprs()], b')'
        # )
        # self._exists_on_db = True
        # TODO: Implementation

    @classmethod
    def _make_tuple(cls, t: Optional[Iterable]) -> tuple:
        if t is None:
            return ()
        if isinstance(t, tuple):
            return t
        return tuple(t)

    @classmethod
    def _join_as_tuple(cls, t1: Optional[Iterable], t2: Optional[Iterable]) -> tuple:
        if t1 is None:
            return cls._make_tuple(t2)
        if t2 is None:
            return cls._make_tuple(t1)
        return (*t1, *t2)

    def _proc_view(self, viewlike: Union['ViewABC', Name]) -> 'ViewABC':
        if isinstance(viewlike, ViewABC):
            return viewlike
        return self.database[viewlike]

    def _proc_terms(self, *exprs: Optional[ExprABC], **coleqs: ExprABC) -> Optional[ExprABC]:
        return OP.AND.call_joined_opt(
            OP.AND.call_joined_opt(*exprs),
            OP.AND.call_joined_opt(*(self.column(n) == v for n, v in coleqs.items()))
        )

    @abstractmethod
    def _new_view(self, *args, **kwargs) -> 'ViewABC':
        """ Make a new view with arguments """


class View(ViewABC):
    """ Table View """
    def __init__(self,
        *_nexprs: Optional[NamedExprABC], 
        database : Optional['Database'] = None,
        name     : Optional[Name] = None,
        froms    : Optional[Iterable[Union['View', Name]]] = None,
        joins    : Optional[Iterable[Tuple[Union['View', Name], JoinLike, Optional[ExprABC]]]] = None,
        where    : Optional[ExprABC] = None,
        groups   : Optional[Iterable[NamedExprABC]] = None,
        orders   : Optional[Iterable[NamedExprABC]] = None,
        limit    : Optional[int] = None,
        offset   : Optional[int] = None,
    ):
        super().__init__(name or b'', *_nexprs, database=database)

        self._froms  : Tuple['Table', ...] = self._make_tuple(froms)
        self._joins  : Tuple[Tuple['Table', JoinType, ExprABC], ...] = self._make_tuple(joins)
        self._where  : Optional[ExprABC] = where
        self._groups : Tuple[NamedExprABC, ...] = self._make_tuple(groups)
        self._orders : Tuple[NamedExprABC, ...] = self._make_tuple(orders)
        self._limit  : Optional[int] = limit
        self._offset : Optional[int] = offset

    def get_froms(self):
        return self._froms

    def get_joins(self):
        return self._joins

    def get_where(self) -> Optional[ExprABC]:
        return self._where

    def get_groups(self) -> Optional[Iterable[NamedExprABC]]:
        return self._groups

    def get_orders(self) -> Optional[Iterable[NamedExprABC]]:
        return self._orders

    def get_limit(self) -> Optional[int]:
        return self._limit

    def get_offset(self) -> Optional[int]:
        return self._offset

    def _new_view(self, *args, **kwargs) -> 'ViewABC':
        return View(*args, **kwargs)


class SingleView(View):
    """ Single result view """

    @property
    def result(self):
        return super().result[0]

    def __dict__(self):
        return dict(self.result)

    def __iter__(self):
        return iter(self.result)

    def __getitem__(self, val):

        if isinstance(val, (bytes, str)):
            val = val if isinstance(val, bytes) else val.encode()
            return self.result[val]

        return super().__getitem__(val)

    def _new_view(self, *args, **kwargs) -> 'View':
        return SingleView(*args, **kwargs)

