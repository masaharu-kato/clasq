"""
    Table abstract classes
"""
from abc import abstractmethod, abstractproperty
from typing import Iterator

from ...syntax.abc.object import ObjectName
from ...syntax.exprs import OP, Arg, ExprABC, NameLike
from ...syntax.values import ValueType
from ...syntax.query_data import QueryData
from ...syntax.errors import ObjectNotFoundError
from ...utils.tabledata import TableData
from ..column import TableColumn, ColumnArgs
from ..fkey_ref import ForeignKeyReference
from .view import NamedViewABC, ViewReferenceABC


class TableArgs:
    """ Table Expr """

    def __init__(self,
        name: NameLike,
        *column_args: ColumnArgs,
        primary_key: tuple[NameLike | TableColumn, ...] | None = None,
        unique: tuple[NameLike | TableColumn, ...] | None = None,
        refs: list[ForeignKeyReference] | None = None,
        # **options
    ):
        self.name = name
        self.column_args = column_args
        self.primary_key = primary_key
        self.unique = unique
        self.refs = refs


class TableABC(NamedViewABC):
    """ Table Expr """
    
    @abstractproperty
    def _primary_keys(self):
        """ Get a primary key """

    @abstractproperty
    def _unique_columns(self):
        """ Get a unique columns """

    def _refresh_select_from_query(self) -> None:
        pass  # Do nothing

    def iter_table_columns(self) -> Iterator[TableColumn]:
        """ Iterate table columns
            (Override from `TableABC`)
        """
        for col in self._base_column_set:
            assert isinstance(col, TableColumn)
            yield col

    def get_table_column(self, val: TableColumn | NameLike) -> TableColumn:
        if isinstance(val, TableColumn):
            if val.table_or_none is not self:
                raise ObjectNotFoundError('Column of the different table.', val)
            return val
        key = ObjectName(val)
        if key not in self._base_column_set:
            raise ObjectNotFoundError('Column not found.', key)
        assert isinstance(col := self._base_column_set[key], TableColumn)
        return col

    def append_to_query_data(self, qd: QueryData) -> None:
        """ Append a query of this table 
            (Override from `QueryABC`)
        """
        qd += self._view_name

    @property
    def _select_from_query_or_none(self) -> QueryData | None:
        """ Get a query of this table for SELECT FROM 
            (Override from `BaseViewABC`)
        """
        return QueryData(self)

    @property
    def _query_for_select_column(self) -> QueryData:
        return QueryData(self, b'.*')

    def select(self, *exprs, **options) -> TableData:
        """ Run SELECT query """
        # TODO: Upgrade with view methods
        return self.clone(*exprs, **options).result

    def insert(self, data: dict[NameLike | TableColumn, ValueType] | None = None, **values: ValueType) -> int:
        """ Run INSERT query

        Args:
            data (Optional[Union[dict[ColumnLike, Any], TableData]], optional): Data to insert. Defaults to None.

        Returns:
            int: Last inserted row ID
        """
        column_values = self._proc_colval_args(data, **values)
        self._con.execute(
            b'INSERT', b'INTO', self, b'(', column_values.keys(), b')',
            b'VALUES', b'(', column_values.values(),  b')',
        )
        return self._con.last_row_id()

    def insert_data(self, data: TableData[ValueType]) -> int:
        """ Run INSERT with TableData """
        name_and_col = [(name, self._to_column(name)) for name in data.columns]
        self._con.execute_many(
            b'INSERT', b'INTO', self, b'(', [col for _, col in name_and_col], b')',
            b'VALUES', b'(', [Arg(name) for name, _ in name_and_col],  b')',
            data=data
        )
        return self._con.last_row_id()

    def update(self,
        data: dict[NameLike | TableColumn, ValueType] | None = None,
        *,
        where: ExprABC | None,
        orders: list[TableColumn] | None = None,
        limit: int | None = None,
        **values: ValueType,
    ) -> None:
        """ Run UPDATE query """

        column_values = self._proc_colval_args(data, **values)
        self._con.execute(
            b'UPDATE', self, b'SET', [(c, b'=', v) for c, v in column_values.items()],
            (b'WHERE', where) if where else None,
            (b'ORDER', b'BY', [c.ordered_query for c in orders]) if orders else None,
            (b'LIMIT', limit) if limit else None,
        )

    def update_data(self, data: TableData[ValueType], keys: list[NameLike | TableColumn]) -> None:
        """ Run UPDATE query with TableData """
        data_name_and_col = [(c, self._to_column(c)) for c in data.columns if c not in keys]
        key_name_and_col  = [(c, self._to_column(c)) for c in data.columns if c in keys]
        if not (len(data_name_and_col) + len(key_name_and_col) == len(data.columns)):
            raise ValueError('Invalid key values.')
        
        self._con.execute_many(
            b'UPDATE', self, b'SET', [(col, b'=', Arg(name)) for name, col in data_name_and_col],
            b'WHERE', OP.AND(col == Arg(name) for name, col in key_name_and_col),
            data=data
        )

    def delete(self, *,
        where: ExprABC | None,
        orders: list[TableColumn] | None = None,
        limit: int | None = None,
    ) -> None:
        """ Run DELETE query """
        self._con.execute(
            b'DELETE', b'FROM', self,
            (b'WHERE', where) if where else None,
            (b'ORDER', b'BY', [c.ordered_query for c in orders]) if orders else None,
            (b'LIMIT', limit) if limit else None,
        )

    def delete_data(self, data: TableData[ValueType]) -> int:
        """ Run DELETE with TableData """
        name_and_col = [(name, self._to_column(name)) for name in data.columns]
        self._con.execute_many(
            b'DELETE', b'FROM', self,
            b'WHERE', OP.AND(col == Arg(name) for name, col in name_and_col),
            data=data
        )
        return self._con.last_row_id()

    def truncate(self) -> None:
        """ Run TRUNCATE TABLE query """
        self.db.execute(b'TRUNCATE', b'TABLE', self)

    def drop(self, *, temporary=False, if_exists=False) -> None:
        """ Run DROP TABLE query """
        self.db.execute(
            b'DROP', b'TEMPORARY' if temporary else None, b'TABLE',
            (b'IF', b'EXISTS') if if_exists else None, self)
        # self._exists_on_db = False
        self.db.remove_table(self)

    def get_create_table_query(self, *, temporary=False, if_not_exists=False) -> QueryData:
        return QueryData(
            b'CREATE', b'TEMPORARY' if temporary else None, b'TABLE',
            b'IF NOT EXISTS' if if_not_exists else None,
            self, b'(', [c.query_for_create_table for c in self.iter_table_columns()], b')'
        )
    
    @property
    def create_table_query(self) -> QueryData:
        return self.get_create_table_query()

    def create(self, *, temporary=False, if_not_exists=False, drop_if_exists=False) -> None:
        """ Create this Table on the database """
        if drop_if_exists:
            self.drop(temporary=temporary, if_exists=True)
        self.db.execute(self.get_create_table_query(temporary=temporary, if_not_exists=if_not_exists))
        # TODO: Fetch

    def __repr__(self) -> str:
        return 'T(%s)' % self.get_name()
        
    def _proc_colval_args(self, value_dict: dict[NameLike | TableColumn, ValueType] | None, **values: ValueType) -> dict[TableColumn, ValueType]:
        return {self.get_table_column(c): v for c, v in [*(value_dict.items() if value_dict else []), *values.items()]}


class TableReferenceABC(ViewReferenceABC, TableABC):
    """ Table Reference abstract class """
    
    @abstractmethod
    def get_entity(self) -> TableABC:
        """ Get a entity table object
            (Abstract property)
        """
        raise NotImplementedError()
    
    @property
    def _entity(self):
        """ Get a entity table object """
        return self.get_entity()

    @property
    def _target_view(self):
        """ Override for `ViewWithTargetABC` """
        return self._entity

    def get_name(self):
        """ Override for `ObjectABC` """
        return self._entity.get_name()

    @property
    def _primary_keys(self):
        """ Override for `TableABC` """
        return self._entity._primary_keys

    @property
    def _unique_columns(self):
        """ Override for `TableABC` """
        return self._entity._unique_columns
