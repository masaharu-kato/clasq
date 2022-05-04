"""
    Schema expression classes
"""
from abc import abstractmethod
from typing import TYPE_CHECKING, Dict, Iterable, Iterator, List, Optional, Tuple, Union

from .syntax.keywords import JoinType, JoinLike, make_join_type, OrderType, ReferenceOption
from .syntax.sqltypebases import SQLType
from .syntax.expr_type import ExprABC, Name, ObjectABC, Object, OrderedABC, OP
from .syntax.query_data import QueryData
from .syntax import errors
from .utils.tabledata import TableData

if TYPE_CHECKING:
    from .database import Database


class Column(Object, OrderedABC):
    """ Column expression """

    def __init__(self,
        name: Name,
        sqltype: Optional[SQLType] = None,
        *,
        view : Optional['ViewABC'] = None,
        table: Optional['Table'] = None,
        default = None, 
        # comment: Optional[str] = None,
        unique: bool = False,
        primary: bool = False,
        auto_increment: bool = False,
        reference: Optional['ForeignKeyReference'] = None,
    ):
        super().__init__(name)
        self._sqltype = sqltype
        self._view = view
        self._table = table
        self._default = default
        # self._comment = comment
        self._unique = unique
        self._primary = primary
        self._auto_increment = auto_increment
        self._reference = reference

    @property
    def sqltype(self) -> Optional[SQLType]:
        return self._sqltype

    @property
    def order_type(self) -> OrderType:
        return OrderType.ASC

    @property
    def original_expr(self) -> ExprABC:
        return self

    @property
    def table(self) -> 'Table':
        if self._table is None:
            raise errors.ObjectNotSetError('Table is not set.')
        return self._table

    @property
    def table_or_none(self):
        return self._table

    @property
    def view(self) -> 'ViewABC':
        if self._view is None:
            return self.table
        return self._view

    @property
    def view_or_none(self):
        return self._view

    @property
    def database(self):
        return self.table.database

    @property
    def cnx(self):
        return self.table.cnx

    @property
    def default(self):
        return self._default

    # @property
    # def comment(self):
    #     return self._comment

    @property
    def is_unique(self):
        return self._unique

    @property
    def is_primary(self):
        return self._primary

    @property
    def is_auto_increment(self):
        return self._auto_increment

    def set_table(self, table: 'Table') -> None:
        """ Set a table object """
        if self._table is not None:
            raise errors.ObjectAlreadySetError('Table already set.')
        self._table = table
    
    def q_create(self) -> tuple:
        return (
            super(Object, self), b' ', self._sqltype,
            (b'DEFAULT', self._default) if self._default else None,
            b'AUTO_INCREMENT' if self._auto_increment else None,
            b'UNIQUE' if self._unique else None,
            b'PRIMARY KEY' if self._primary else None,
            self._reference,
        )

    def q_order(self) -> tuple:
        return (self, self.order_type)

    def q_select(self) -> tuple:
        return (self,)

    def append_query_data(self, qd: 'QueryData') -> None:
        if self.table_or_none:
            qd.append(self.table, b'.')
        super().append_query_data(qd)

    def __pos__(self):
        """ Get a ASC ordered expression """
        return OrderedColumn(self, OrderType.ASC)

    def __neg__(self):
        """ Get a DESC ordered expression """
        return OrderedColumn(self, OrderType.DESC)

    def __repr__(self):
        return 'Col(%s)' % str(self)


ColumnLike = Union[str, bytes, Column]


class OrderedColumn(OrderedABC):
    """ Ordered Column Expr """
    def __init__(self, expr: Column, order: OrderType):
        self._original_expr = expr
        self._order_kind = order

    @property
    def original_expr(self) -> ExprABC:
        """ Get a original expr """
        return self._original_expr

    @property
    def order_type(self) -> OrderType:
        return self._order_kind

    def iter_objects(self) -> Iterator[Object]:
        return self._original_expr.iter_objects()



class ViewABC(Object):
    """ View Expr """

    def __init__(self,
        name: Name,
        database: Optional['Database'] = None,
        *_columns: Optional[ObjectABC],
    ):
        super().__init__(name)
        columns = [c for c in _columns if c is not None]
        self._column_specified = bool(columns)
        self._column_dict: Dict[bytes, ObjectABC] = {}
        self._database = database
        # self._options = options
        for column in columns:
            self.append_column(column)
            
        self._result : Optional[TableData] = None # View Result

    def __repr__(self):
        return 'View(%s)' % str(self)

    @property
    def column_specified(self):
        return self._column_specified

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

    # @property
    # def options(self):
    #     return self._options

    def iter_columns(self) -> Iterator[ObjectABC]:
        if self._column_specified:
            return (c for c in self._column_dict.values())
        return iter([])

    def set_database(self, database: 'Database') -> None:
        """ Set a table object """
        if self._database is not None:
            raise errors.ObjectAlreadySetError('Database already set.')
        self._database = database

    def column(self, val: ColumnLike):

        if isinstance(val, (bytes, str)):
            name = val.encode() if isinstance(val, str) else val
            if name not in self._column_dict:
                if not self.column_specified:
                    self._column_dict[name] = Column(name, view=self)
                else:
                    raise errors.ObjectNotFoundError('Undefined column name `%r` on view `%r`' % (name, self._name))
            return self._column_dict[name]
            
        if isinstance(val, Column):
            if val.view_or_none == self:
                return val
            raise errors.NotaSelfObjectError('Not a column of this view.')

        raise errors.ObjectArgumentsTypeError('Invalid type %s (%s)' % (type(val), val))

    def col(self, val: ColumnLike):
        return self.column(val)

    def append_column(self, column: ObjectABC) -> None:
        self._column_dict[column.name] = column
        self._column_specified = True


    def get_froms(self) -> Optional[Iterable[Union['View', Name]]]:
        return None # Default Implementation

    def get_joins(self) -> Optional[Iterable[Tuple[Union['View', Name], JoinLike, Optional[ExprABC]]]]:
        return None # Default Implementation

    def get_where(self) -> Optional[ExprABC]:
        return None # Default Implementation

    def get_groups(self) -> Optional[Iterable[Column]]:
        return None # Default Implementation

    def get_orders(self) -> Optional[Iterable[OrderedColumn]]:
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
        groups   : Optional[Iterable[Column]] = None,
        orders   : Optional[Iterable[OrderedColumn]] = None,
        limit    : Optional[int] = None,
        offset   : Optional[int] = None,
    ) -> 'ViewABC':

        return self._new_view(
            *self.iter_columns(), *_columns,
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
            *view.iter_columns(),
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

    def group_by(self, *columns: Column) -> 'ViewABC':
        return self.clone(groups=columns)

    def order_by(self, *orders: OrderedColumn) -> 'ViewABC':
        """ Make a View object with ORDER BY clause """
        return self.clone(orders=orders)

    def limit(self, limit: int) -> 'ViewABC':
        """ Make a View object with LIMIT OFFSET clause """
        return self.clone(limit=limit)

    def offset(self, offset: int) -> 'ViewABC':
        """ Make a View object with LIMIT OFFSET clause """
        return self.clone(offset=offset)

    # def single(self) -> 'SingleView':
    #     """ Make a View object with a single result """
    #     return SingleView(self)

    def join(self, join_type: JoinLike, view: Union['ViewABC', Name], *exprs: ExprABC, **_coleqs: ColumnLike) -> 'ViewABC':
        """ Make a Joined View """
        coleqs = {n: self.column(v) for n, v in _coleqs.items()}
        return self.clone(joins=[(
            self._proc_view(view),
            make_join_type(join_type),
            self._proc_terms(*exprs, **coleqs)
        )])

    def inner_join(self, view: Union['ViewABC', Name], *exprs: ExprABC, **coleqs: ColumnLike) -> 'ViewABC':
        """ Make a INNER Joined View """
        return self.join(JoinType.INNER, view, *exprs, **coleqs)

    def left_join(self, view: Union['ViewABC', Name], *exprs: ExprABC, **coleqs: ColumnLike) -> 'ViewABC':
        """ Make a LEFT Joined View """
        return self.join(JoinType.LEFT, view, *exprs, **coleqs)

    def right_join(self, view: Union['ViewABC', Name], *exprs: ExprABC, **coleqs: ColumnLike) -> 'ViewABC':
        """ Make a RIGHT Joined View """
        return self.join(JoinType.RIGHT, view, *exprs, **coleqs)

    def outer_join(self, view: Union['ViewABC', Name], *exprs: ExprABC, **coleqs: ColumnLike) -> 'ViewABC':
        """ Make a OUTER Joined View """
        return self.join(JoinType.OUTER, view, *exprs, **coleqs)

    def cross_join(self, view: Union['ViewABC', Name], *exprs: ExprABC, **coleqs: ColumnLike) -> 'ViewABC':
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

    
    def refresh_result(self) -> None:
        self._result = self.db.select(
            *self.iter_columns(),
            froms  = self.get_froms(),
            joins  = self.get_joins(),
            where  = self.get_where(),
            groups = self.get_groups(),
            orders = self.get_orders(),
            limit  = self.get_limit(),
            offset = self.get_offset(),
        )

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
        *_columns: Optional[ObjectABC], 
        database : Optional['Database'] = None,
        name     : Optional[Name] = None,
        froms    : Optional[Iterable[Union['View', Name]]] = None,
        joins    : Optional[Iterable[Tuple[Union['View', Name], JoinLike, Optional[ExprABC]]]] = None,
        where    : Optional[ExprABC] = None,
        groups   : Optional[Iterable[Column]] = None,
        orders   : Optional[Iterable[OrderedColumn]] = None,
        limit    : Optional[int] = None,
        offset   : Optional[int] = None,
    ):
        super().__init__(name or b'', database, *_columns)
        self._froms  : Tuple['Table', ...] = self._make_tuple(froms)
        self._joins  : Tuple[Tuple['Table', JoinType, ExprABC], ...] = self._make_tuple(joins)
        self._where  : Optional[ExprABC] = where
        self._groups : Tuple[Column, ...] = self._make_tuple(groups)
        self._orders : Tuple[OrderedColumn, ...] = self._make_tuple(orders)
        self._limit  : Optional[int] = limit
        self._offset : Optional[int] = offset

    def get_froms(self):
        return self._froms

    def get_joins(self):
        return self._joins

    def get_where(self) -> Optional[ExprABC]:
        return self._where

    def get_groups(self) -> Optional[Iterable[Column]]:
        return self._groups

    def get_orders(self) -> Optional[Iterable[OrderedColumn]]:
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


class Table(ViewABC):
    """ Table Expr """

    def __init__(self,
        name: Name,
        database: Optional['Database'] = None,
        *columns: ObjectABC,
        # **options
    ):
        super().__init__(name, database, *columns) #, **options)

    def __repr__(self):
        return 'Table(%s)' % str(self)

    def append_column(self, column: ObjectABC) -> None:
        if not isinstance(column, Column):
            raise errors.ObjectArgumentsTypeError('Invalid argument type %s (%s)' % (type(column), column))

        if column.table_or_none:
            if not column.table == self:
                raise errors.NotaSelfObjectError('Column of the different table.')
        else:
            column.set_table(self)
        return super().append_column(column)

    def column(self, val: ColumnLike):

        if isinstance(val, (bytes, str)):
            name = val.encode() if isinstance(val, str) else val
            if name not in self._column_dict:
                if not self.column_specified:
                    self._column_dict[name] = Column(name, table=self)
                else:
                    raise errors.ObjectNotFoundError('Unknown column name `%r` on table `%r`' % (name, self._name))
            return self._column_dict[name]
            
        if isinstance(val, ObjectABC):
            if val.table_or_none == self:
                return val
            raise errors.NotaSelfObjectError('Not a column of this table.')

        raise errors.ObjectArgumentsTypeError('Invalid type %s (%s)' % (type(val), val))
        
    def q_select(self) -> tuple:
        return (self, b'.*')

    def q_create(self, *, temporary=False, if_not_exists=False) -> tuple:
        return (
            b'CREATE', b'TEMPORARY' if temporary else None, b'TABLE',
            b'IF NOT EXISTS' if if_not_exists else None,
            self, b'(', [c.q_create() for c in self.iter_objects()], b')'
        )
        # TODO: Add table options

    def select(self, *exprs, **options) -> TableData:
        """ Run SELECT query """
        return self.cnx.select(self, *exprs, **options)

    def insert(self, data, **values) -> int:
        """ Run INSERT query """
        return self.cnx.insert(self, data, **values)

    def update(self, data, **options):
        """ Run UPDATE query """
        return self.cnx.update(self, data, **options)

    def delete(self, **options):
        """ Run DELETE query """
        return self.cnx.delete(self, **options)

    def get_froms(self):
        return (self,)
        
    def _new_view(self, *args, **kwargs) -> 'ViewABC':
        return View(*args, **kwargs)


TableLike = Union[str, bytes, Table]

class ForeignKeyReference(Object):
    """ Foreign Key Reference """

    def __init__(self,
        orig_column: Union[Column, Tuple[Column, ...]],
        ref_column : Union[Column, Tuple[Column, ...]],
        *,
        on_delete: Optional[ReferenceOption] = None,
        on_update: Optional[ReferenceOption] = None,
        name: Optional[Name] = None
    ):
        super().__init__(name or b'')
        
        _orig_columns = orig_column if isinstance(orig_column, (tuple, list)) else [orig_column]
        _ref_columns  = ref_column  if isinstance(ref_column , (tuple, list)) else [ref_column]
        assert len(_orig_columns) and _orig_columns[0].table is not None
        assert len(_ref_columns ) and _ref_columns [0].table is not None

        self._orig_table = _orig_columns[0].table
        self._ref_table = _ref_columns[0].table
        assert all(self._orig_table == c.table for c in _orig_columns)
        assert all(self._ref_table  == c.table for c in _ref_columns)

        self._orig_columns = orig_column if isinstance(orig_column, (tuple, list)) else [orig_column]
        self._ref_columns  = ref_column  if isinstance(ref_column , (tuple, list)) else [ref_column]
        self._on_delete = on_delete
        self._on_update = on_update

    @property
    def on_delete(self):
        return self._on_delete

    @property
    def on_update(self):
        return self._on_update

    def append_query_data(self, qd: QueryData) -> None:
        """ Append this to query data"""
        qd.append(
            b'FOREIGN', b'KEY', self.name, b'(', [super(Object, c) for c in self._orig_columns], b')',
            b'REFERENCES', self._ref_table, b'(', [super(Object, c) for c in self._ref_columns], b')',
            (b'ON', b'DELETE', self._on_delete) if self._on_delete else None,
            (b'ON', b'UPDATE', self._on_update) if self._on_update else None,
        )


def iter_objects(*exprs: Optional[ExprABC]):
    for expr in exprs:
        if expr is not None:
            yield from expr.iter_objects()


def iter_columns(*exprs: Optional[ExprABC]):
    for e in iter_objects(*exprs):
        if isinstance(e, Column):
            yield e

def iter_tables(*exprs: Optional[ExprABC]):
    for e in iter_objects(*exprs):
        if isinstance(e, Column):
            if e.table_or_none:
                yield e.table
        elif isinstance(e, Table):
            yield e
