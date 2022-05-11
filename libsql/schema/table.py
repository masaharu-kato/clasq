"""
    Table classes
"""
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Tuple, Union

from ..syntax.keywords import ReferenceOption
from ..syntax.object_abc import FrozenObjectSet, OrderedFrozenObjectSet, to_name
from ..syntax.exprs import OP, Arg, ExprABC, NamedExprABC, ObjectABC, Object, Name
from ..syntax.values import ValueType
from ..syntax.query_abc import iter_objects
from ..syntax.query_data import QueryData
from ..syntax import sqltypes
from ..syntax import errors
from ..utils.tabledata import TableData
from .column import TableColumn, TableColumnArgs

if TYPE_CHECKING:
    from .database import Database


class TableArgs:
    """ Table Expr """

    def __init__(self,
        name: NameLike,
        *column_args: TableColumnArgs,
        primary_key: Optional[Tuple[Union[NameLike, TableColumn], ...]] = None,
        unique: Optional[Tuple[Union[NameLike, TableColumn], ...]] = None,
        refs: Optional[List['ForeignKeyReference']] = None,
        # **options
    ):
        self.name = name
        self.column_args = column_args
        self.primary_key = primary_key
        self.unique = unique
        self.refs = refs


# class Table(NamedViewABC, ViewWithColumns, Object): # <-- super() is not working correctly on these base classes
class Table(TableViewABC):
    """ Table Expr """

    def __init__(self, database: 'Database', args: TableArgs):
        self._name = ObjectName(args.name)
        self._database = database
        self._table_columns = [TableColumn(self, colargs) for colargs in args.column_args]
        self._table_columns_by_name = {col.name: col for col in self._table_columns}
        super().__init__(self._table_columns_by_name)
        
        self._primary_key: Optional[List[TableColumn]] = None
        self._unique: Optional[List[TableColumn]] = None
        self._refs = args.refs

        self._primary_key = [self.table_column(c) for c in (args.primary_key or ())]
        self._unique = [self.table_column(c) for c in (args.unique or ())]

    def refresh_select_from_query(self) -> None:
        pass  # Do nothing

    @property
    def name_or_none(self) -> Optional[ObjectName]:
        """ Get a view name 
            (Override from `ViewABC` """
        return self._name
    
    def table_column(self, val: Union[TableColumn, NameLike]) -> TableColumn:
        if isinstance(val, TableColumn):
            if val.table_or_none is not self:
                raise errors.ObjectNotFoundError('Column of the different table.', val)
            return val
        key = ObjectName(val)
        if key not in self._table_columns_by_name:
            raise errors.ObjectNotFoundError('Column not found.', key)
        return self._table_columns_by_name[key]

    def append_to_query_data(self, qd: 'QueryData') -> None:
        """ Append a query of this table 
            (Override from `QueryABC`)
        """
        qd += self.name

    @property
    def select_from_query_or_none(self) -> Optional[QueryData]:
        """ Get a query of this table for SELECT FROM 
            (Override from `BaseViewABC`)
        """
        return QueryData(self)

    @property
    def database_or_none(self) -> Optional['Database']:
        """ Get a parent Database object 
            (Override from `ViewABC`)
        """
        return self._database

    @property
    def query_for_select_column(self) -> QueryData:
        return QueryData(self, b'.*')

    def select(self, *exprs, **options) -> TableData:
        """ Run SELECT query """
        # TODO: Upgrade with view methods
        return self.clone(*exprs, **options).result

    def insert(self, data: Optional[Dict[Union[Name, Column], ValueType]] = None, **values: ValueType) -> int:
        """ Run INSERT query

        Args:
            data (Optional[Union[Dict[ColumnLike, Any], TableData]], optional): Data to insert. Defaults to None.

        Returns:
            int: Last inserted row ID
        """
        column_values = self._proc_colval_args(data, **values)
        self.cnx.execute(
            b'INSERT', b'INTO', self, b'(', column_values.keys(), b')',
            b'VALUES', b'(', column_values.values(),  b')',
        )
        return self.cnx.last_row_id()

    def insert_data(self, data: TableData[ValueType]) -> int:
        """ Run INSERT with TableData """
        name_and_col = [(name, self.table_column(name)) for name in data.columns]
        self.cnx.execute_many(
            b'INSERT', b'INTO', self, b'(', [col for _, col in name_and_col], b')',
            b'VALUES', b'(', [Arg(name) for name, _ in name_and_col],  b')',
            data=data
        )
        return self.cnx.last_row_id()

    def update(self,
        data: Optional[Dict[Union[Name, Column], ValueType]] = None,
        *,
        where: Optional[ExprABC],
        orders: Optional[List[Column]] = None,
        limit: Optional[int] = None,
        **values: ValueType,
    ) -> None:
        """ Run UPDATE query """

        column_values = self._proc_colval_args(data, **values)
        self.cnx.execute(
            b'UPDATE', self, b'SET', [(c, b'=', v) for c, v in column_values.items()],
            (b'WHERE', where) if where else None,
            (b'ORDER', b'BY', [c.q_order() for c in orders]) if orders else None,
            (b'LIMIT', limit) if limit else None,
        )


    def update_data(self, data: TableData[ValueType], keys: List[Union[Name, Column]]) -> None:
        """ Run UPDATE query with TableData """
        data_name_and_col = [(c, self.table_column(c)) for c in data.columns if c not in keys]
        key_name_and_col  = [(c, self.table_column(c)) for c in data.columns if c in keys]
        if not (len(data_name_and_col) + len(key_name_and_col) == len(data.columns)):
            raise ValueError('Invalid key values.')
        
        self.cnx.execute_many(
            b'UPDATE', self, b'SET', [(col, b'=', Arg(name)) for name, col in data_name_and_col],
            b'WHERE', OP.AND(col == Arg(name) for name, col in key_name_and_col),
            data=data
        )

    def delete(self, *,
        where: Optional[ExprABC],
        orders: Optional[List[Column]] = None,
        limit: Optional[int] = None,
    ) -> None:
        """ Run DELETE query """
        self.cnx.execute(
            b'DELETE', b'FROM', self,
            (b'WHERE', where) if where else None,
            (b'ORDER', b'BY', [c.q_order() for c in orders]) if orders else None,
            (b'LIMIT', limit) if limit else None,
        )

    def delete_data(self, data: TableData[ValueType]) -> int:
        """ Run DELETE with TableData """
        name_and_col = [(name, self.table_column(name)) for name in data.columns]
        self.cnx.execute_many(
            b'DELETE', b'FROM', self,
            b'WHERE', OP.AND(col == Arg(name) for name, col in name_and_col),
            data=data
        )
        return self.cnx.last_row_id()

    def truncate(self) -> None:
        """ Run TRUNCATE TABLE query """
        self.db.execute(b'TRUNCATE', b'TABLE', self)

    def drop(self, *, temporary=False, if_exists=False) -> None:
        """ Run DROP TABLE query """
        if_exists = if_exists or (not self._exists_on_db)
        self.db.execute(
            b'DROP', b'TEMPORARY' if temporary else None, b'TABLE',
            (b'IF', b'EXISTS') if if_exists else None, self)
        self._exists_on_db = False
        self.db.remove_table(self)

    def create(self, *, temporary=False, if_not_exists=False, drop_if_exists=False) -> None:
        """ Create this Table on the database """
        if drop_if_exists:
            self.drop(temporary=temporary, if_exists=True)
        self.db.execute(
            b'CREATE', b'TEMPORARY' if temporary else None, b'TABLE',
            b'IF NOT EXISTS' if if_not_exists else None,
            self, b'(', [c.query_for_create_table for c in self.columns], b')'
        )
        self.fetch_from_db()
        
    def _new_view(self, *args, **kwargs) -> 'ViewABC':
        return View(*args, **kwargs)
        
    def _proc_colval_args(self, value_dict: Optional[Dict[Union[Name, Column], ValueType]], **values: ValueType) -> Dict[Column, ValueType]:
        return {self.table_column(c): v for c, v in [*(value_dict.items() if value_dict else []), *values.items()]}

    def __repr__(self):
        return 'Table(%s)' % str(self)


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

    def append_to_query_data(self, qd: QueryData) -> None:
        """ Append this to query data"""
        qd.append(
            b'FOREIGN', b'KEY', self.name, b'(', [super(Object, c) for c in self._orig_columns], b')',
            b'REFERENCES', self._ref_table, b'(', [super(Object, c) for c in self._ref_columns], b')',
            (b'ON', b'DELETE', self._on_delete) if self._on_delete else None,
            (b'ON', b'UPDATE', self._on_update) if self._on_update else None,
        )


def iter_tables(*exprs: Optional[ObjectABC]):
    for e in iter_objects(*exprs):
        if isinstance(e, Column):
            if e.table_or_none is not None:
                yield e.table
        elif isinstance(e, Table):
            yield e
