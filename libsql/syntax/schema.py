"""
    Schema expression classes
"""
from abc import abstractmethod, abstractproperty
from typing import Optional, Tuple, Union

from .keywords import OrderType, ReferenceOption
from .expr_abc import ExprABC
from .expr_type import ExprType, ObjectExprABC, ObjectExpr, OrderedABC
from .sqltypebases import SQLType
from .query_data import QueryData


class Column(ObjectExpr, OrderedABC):
    """ Column expression """

    def __init__(self,
        name: bytes,
        sqltype: Optional[SQLType] = None,
        *,
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
    def order_kind(self) -> OrderType:
        return OrderType.ASC

    @property
    def original_expr(self) -> ExprABC:
        return self

    @property
    def table(self) -> Optional['Table']:
        return self._table

    def table_expr(self):
        return self.table

    def database_expr(self):
        return self.table.database

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
    
    def q_create(self) -> tuple:
        return (
            super(ObjectExpr, self), b' ', self._sqltype,
            (b'DEFAULT', self._default) if self._default else None,
            b'AUTO_INCREMENT' if self._auto_increment else None,
            b'UNIQUE' if self._unique else None,
            b'PRIMARY KEY' if self._primary else None,
            self._reference,
        )

    def q_order(self) -> tuple:
        return (self, self.order_kind)

    def q_select(self) -> tuple:
        return (self,)

    @property
    def stmt_bytes(self) -> bytes:
        return (self.table.stmt_bytes + b'.' if self.table else b'') + super().stmt_bytes

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

    @abstractproperty
    def original_expr(self) -> ExprType:
        """ Get a original expr """
        return self._original_expr

    @property
    def order_kind(self) -> OrderType:
        return self._order_kind


class View(ObjectExpr):
    """ View Expr """

    def __init__(self,
        name: bytes,
        *columns: ObjectExprABC,
        database: Optional['Database'] = None,
        **options
    ):
        super().__init__(name)
        self._column_specified = bool(columns)
        self._column_dict = {c.name: c for c in columns} if columns else {}
        self._database = database
        self._options = options

    def iter_columns(self):
        return self._column_dict.values()

    @property
    def column_specified(self):
        return self._column_specified

    @property
    def database(self):
        return self._database

    @property
    def options(self):
        return self._options

    def __repr__(self):
        return 'View(%s)' % str(self)

    def column(self, name: bytes):
        if name not in self._column_dict:
            if not self.column_specified:
                self._column_dict[name] = Column(name, table=self)
            else:
                raise KeyError('Undefined column name `%r` on table `%r`' % (name, self._name))
        return self._column_dict[name] 

    def col(self, name: bytes):
        return self.column(name)
        
    def __getitem__(self, val: ColumnLike):
        if isinstance(val, str):
            return self.column(val.encode())
        if isinstance(val, bytes):
            return self.column(val)
        if isinstance(val, Column):
            if val.table == self:
                return val
            raise RuntimeError('Not a column of this table.')
        raise TypeError('Invalid type %s (%s)' % (type(val), val))

    def table_expr(self):
        return self


class Table(View):
    """ Table Expr """

    def __repr__(self):
        return 'Table(%s)' % str(self)

    def database_expr(self):
        return self.database
        
    def q_select(self) -> tuple:
        return (self, b'.*')

    def q_create(self, *, temporary=False, if_not_exists=False) -> tuple:
        return (
            b'CREATE', b'TEMPORARY' if temporary else None, b'TABLE',
            b'IF NOT EXISTS' if if_not_exists else None,
            self, b'(', [c.q_create() for c in self.iter_columns()], b')'
        )
        # TODO: Add table options

TableLike = Union[str, bytes, Table]

class ForeignKeyReference(ObjectExpr):
    def __init__(self,
        orig_column: Union[Column, Tuple[Column, ...]],
        ref_column : Union[Column, Tuple[Column, ...]],
        *,
        on_delete: Optional[ReferenceOption] = None,
        on_update: Optional[ReferenceOption] = None,
        name: Optional[bytes] = None
    ):
        super().__init__(name)
        
        _orig_columns = orig_column if isinstance(orig_column, (tuple, list)) else [orig_column]
        _ref_columns  = ref_column  if isinstance(ref_column , (tuple, list)) else [ref_column]
        assert len(_orig_columns) and _orig_columns[0].table is not None
        assert len(_ref_columns ) and _ref_columns [0].table is not None

        self._orig_table = _orig_columns[0].table
        self._ref_table = _ref_columns[0].table
        assert all(self._orig_table == c.table for c in _orig_columns)
        assert all(self._ref_table  == c.table for c in _ref_columns)

        self._orig_columns = orig_column
        self._ref_columns = ref_column
        self._on_delete = on_delete
        self._on_update = on_update

    def append_query_data(self, qd: QueryData) -> None:
        """ Append this to query data"""
        qd.append(
            b'FOREIGN', b'KEY', self.name, b'(', [super(ObjectExpr, c) for c in self._orig_columns], b')',
            b'REFERENCES', self._ref_table, b'(', [super(ObjectExpr, c) for c in self._ref_columns], b')',
            (b'ON', b'DELETE', self._on_delete) if self.on_delete else None,
            (b'ON', b'UPDATE', self._on_update) if self.on_update else None,
        )


class Database(ObjectExpr):
    """ Database Expr """

    def __init__(self, name: bytes, *tables: Table, **options):
        super().__init__(name)
        self._table_specified = bool(tables)
        self._table_dict = {c.name: c for c in tables} if tables else {}
        self._options = options

    def iter_tables(self):
        return self._table_dict.values()

    @property
    def table_specified(self):
        return self._table_specified

    @property
    def options(self):
        return self._options

    def __repr__(self):
        return 'DB(%s)' % str(self)

    def table(self, name: bytes):
        if name not in self._table_dict:
            if not self.table_specified:
                self._table_dict[name] = Table(name, database=self)
            else:
                raise KeyError('Undefined column name `%r` on table `%r`' % (name, self._name))
        return self._table_dict[name] 
        
    def __getitem__(self, val: TableLike):
        if isinstance(val, str):
            return self.table(val.encode())
        if isinstance(val, bytes):
            return self.table(val)
        if isinstance(val, Table):
            if val.database == self:
                return val
            raise RuntimeError('Not a table of this database.')
        raise TypeError('Invalid type of value.')

    def database_expr(self) -> 'Database':
        return self

    def q_create(self, *, if_not_exists=False) -> tuple:
        return (
            b'CREATE', b'DATABASE',
            (b'IF', b'NOT', b'EXISTS') if if_not_exists else None,
            self
        )
        # TODO: Add database options