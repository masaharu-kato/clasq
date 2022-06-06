"""
    Table abstract classes
"""
from __future__ import annotations
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Collection, Iterator

from ...syntax.abc.query import QueryDataABC
from ...syntax.abc.object import ObjectName
from ...syntax.abc.values import SQLValue
from ...syntax.exprs import ExprABC, NameLike, QueryArg, OPs
from ...syntax.query import QueryData
from ...errors import ObjectNotFoundError
from ...utils.tabledata import TableData
from .view import NamedViewABC, ViewReferenceABC
from .column import TableColumnABC, TableColumnArgs

if TYPE_CHECKING:
    from .fkey_ref import ForeignKeyReference


@dataclass
class TableArgs:
    """ Table Expr """
    name: NameLike
    column_args: Collection[TableColumnArgs]
    primary_key: tuple[NameLike | TableColumnABC, ...] | None = None
    unique: tuple[NameLike | TableColumnABC, ...] | None = None
    refs: list[ForeignKeyReference] | None = None


class TableABC(NamedViewABC):
    """ Table Expr """
    
    # @abstractproperty
    # def _primary_keys(self):
    #     """ Get a primary key """

    # @abstractproperty
    # def _unique_columns(self):
    #     """ Get a unique columns """

    def _refresh_select_from_query(self) -> None:
        pass  # Do nothing

    def iter_table_columns(self) -> Iterator[TableColumnABC]:
        """ Iterate table columns
            (Override from `TableABC`)
        """
        for col in self._base_column_set:
            assert isinstance(col, TableColumnABC)
            yield col

    def get_table_column(self, val: TableColumnABC | NameLike) -> TableColumnABC:
        if isinstance(val, TableColumnABC):
            if val.table is not self:
                raise ObjectNotFoundError('Column of the different table.', val)
            return val
        key = ObjectName(val)
        if key not in self._base_column_set:
            raise ObjectNotFoundError('Column not found.', key)
        assert isinstance(col := self._base_column_set[key], TableColumnABC)
        return col

    def _append_to_query_data(self, qd: QueryDataABC) -> None:
        """ Append a query of this table 
            (Override from `QueryABC`)
        """
        qd.append(self._view_name)

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

    def insert(self, data: dict[NameLike | TableColumnABC, SQLValue] | None = None, **values: SQLValue) -> int:
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

    def insert_data(self, data: TableData[SQLValue]) -> int:
        """ Run INSERT with TableData """
        name_and_col = [(name, self._to_column(name)) for name in data.columns]
        self._con.execute_many(
            b'INSERT', b'INTO', self, b'(', [col for _, col in name_and_col], b')',
            b'VALUES', b'(', [QueryArg(name) for name, _ in name_and_col],  b')',
            data=data
        )
        return self._con.last_row_id()

    def update(self,
        data: dict[NameLike | TableColumnABC, SQLValue] | None = None,
        *,
        where: ExprABC | None,
        orders: list[TableColumnABC] | None = None,
        limit: int | None = None,
        **values: SQLValue,
    ) -> None:
        """ Run UPDATE query """

        column_values = self._proc_colval_args(data, **values)
        self._con.execute(
            b'UPDATE', self, b'SET', [(c, b'=', v) for c, v in column_values.items()],
            (b'WHERE', where) if where else (),
            (b'ORDER', b'BY', [c._ordered_query for c in orders]) if orders else (),
            (b'LIMIT', limit) if limit else (),
        )

    def update_data(self, data: TableData[SQLValue], keys: list[NameLike | TableColumnABC]) -> None:
        """ Run UPDATE query with TableData """
        data_name_and_col = [(c, self._to_column(c)) for c in data.columns if c not in keys]
        key_name_and_col  = [(c, self._to_column(c)) for c in data.columns if c in keys]
        if not (len(data_name_and_col) + len(key_name_and_col) == len(data.columns)):
            raise ValueError('Invalid key values.')
        
        self._con.execute_many(
            b'UPDATE', self, b'SET', [(col, b'=', QueryArg(name)) for name, col in data_name_and_col],
            b'WHERE', OPs.AND(col == QueryArg(name) for name, col in key_name_and_col),
            data=data
        )

    def delete(self, *,
        where: ExprABC | None,
        orders: list[TableColumnABC] | None = None,
        limit: int | None = None,
    ) -> None:
        """ Run DELETE query """
        self._con.execute(
            b'DELETE', b'FROM', self,
            (b'WHERE', where) if where else (),
            (b'ORDER', b'BY', [c._ordered_query for c in orders]) if orders else (),
            (b'LIMIT', limit) if limit else (),
        )

    def delete_data(self, data: TableData[SQLValue]) -> int:
        """ Run DELETE with TableData """
        name_and_col = [(name, self._to_column(name)) for name in data.columns]
        self._con.execute_many(
            b'DELETE', b'FROM', self,
            b'WHERE', OPs.AND(col == QueryArg(name) for name, col in name_and_col),
            data=data
        )
        return self._con.last_row_id()

    def truncate(self) -> None:
        """ Run TRUNCATE TABLE query """
        self.db.execute(b'TRUNCATE', b'TABLE', self)

    def drop(self, *, temporary=False, if_exists=False) -> None:
        """ Run DROP TABLE query """
        self.db.execute(
            b'DROP', b'TEMPORARY' if temporary else (), b'TABLE',
            (b'IF', b'EXISTS') if if_exists else (), self)
        # self._exists_on_db = False
        self.db.remove_table(self)

    def get_create_table_query(self, *, temporary=False, if_not_exists=False) -> QueryData:
        return QueryData(
            b'CREATE', b'TEMPORARY' if temporary else (), b'TABLE',
            (b'IF', b'NOT', b'EXISTS') if if_not_exists else (),
            self, b'(', [c._query_for_create_table for c in self.iter_table_columns()], b')'
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
    
    @property
    def python_class_source(self) -> str:
        res = 'class %s(TableClass, db=%s):\n' % (self._name, self.db._name)
        for column in self.iter_table_columns():
            res += '%s: %s\n' % (column.name, column._data_type.annotation)
        return res

    def __repr__(self) -> str:
        return 'T(%s)' % self._name
        
    def _proc_colval_args(self, value_dict: dict[NameLike | TableColumnABC, SQLValue] | None, **values: SQLValue) -> dict[TableColumnABC, SQLValue]:
        return {self.get_table_column(c): v for c, v in [*(value_dict.items() if value_dict else []), *values.items()]}


class TableReferenceABC(ViewReferenceABC, TableABC):
    """ Table Reference abstract class """
    
    @abstractmethod
    def _get_dest_table(self) -> TableABC:
        """ Get a entity table object
            (Abstract property)
        """
        raise NotImplementedError()

    @abstractmethod
    def _get_name(self) -> ObjectName:
        """ Get a table name to reference
            (Abstract property)
        """
        
    @property
    def _dest_table(self):
        """ Get a entity table object """
        return self._get_dest_table()

    @property
    def _target_view(self):
        """ Override for `ViewWithTargetABC` """
        return self._dest_table

    @property
    def _name(self):
        """ Override for `ObjectABC` """
        return self._get_name()

    @property
    def _primary_keys(self):
        """ Override for `TableABC` """
        return self._dest_table._primary_keys

    @property
    def _unique_columns(self):
        """ Override for `TableABC` """
        return self._dest_table._unique_columns
