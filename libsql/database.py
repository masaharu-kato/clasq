"""
    Database class definition
"""
from typing import TYPE_CHECKING, Any, Dict, Optional, List, Tuple, Set

from .schema import Object, Table, TableLike, Name
from .syntax.exprs import ExprABC, ObjectABC
from .schema import Table, TableLike, Column, ColumnLike, OrderedColumn, iter_tables
from .syntax.keywords import JoinType, make_join_type
from .syntax import errors
from .utils.tabledata import TableData

if TYPE_CHECKING:
    from .connection import ConnectionABC
    

class Database(Object):
    """ Database Expr """

    def __init__(self, name: Name, *tables: Table, cnx: Optional['ConnectionABC'] = None, **options):
        super().__init__(name)
        self._table_specified = bool(tables)
        self._table_dict: Dict[bytes, Table] = {}
        self._cnx = cnx
        self._options = options
        for table in tables:
            self.append_table(table)

    @property
    def cnx(self):
        if self._cnx is None:
            raise errors.ObjectNotSetError('Connection is not set.')
        return self._cnx

    @property
    def table_specified(self):
        return self._table_specified

    @property
    def options(self):
        return self._options

    @property
    def database(self) -> 'Database':
        return self

    def __repr__(self):
        return 'DB(%s)' % str(self)

    def iter_tables(self):
        return iter(self._table_dict.values())

    def table(self, val: TableLike):
        if isinstance(val, (bytes, str)):
            name = val.encode() if isinstance(val, str) else val
            if name not in self._table_dict:
                if not self.table_specified:
                    self._table_dict[name] = Table(name, database=self)
                else:
                    raise errors.ObjectNotFoundError('Undefined column name `%r` on table `%r`' % (name, self._name))
            return self._table_dict[name] 

        if isinstance(val, Table):
            if val.database == self:
                return val
            raise errors.NotaSelfObjectError('Not a table of this database.')

        raise errors.ObjectArgsError('Invalid type %s (%s)' % (type(val), val))
        
    def __getitem__(self, val: TableLike):
        return self.table(val)

    def append_table(self, table: Table) -> None:
        if table.database_or_none:
            if not table.database == self:
                raise errors.NotaSelfObjectError('Table of the different database.')
        else:
            table.set_database(self)
        self._table_dict[table.name] = table


    def q_create(self, *, if_not_exists=False) -> tuple:
        return (
            b'CREATE', b'DATABASE',
            (b'IF', b'NOT', b'EXISTS') if if_not_exists else None,
            self
        )
        # TODO: Add database options

    @property
    def last_qd(self):
        return self.cnx.last_qd

    def execute(self, *args, **kwargs) -> None:
        return self.cnx.execute(*args, **kwargs)

    def query(self, *args, **kwargs) -> TableData:
        return self.cnx.query(*args, **kwargs)

    def select(self,
        *_columns_or_tables: Optional[ObjectABC],
        froms : Optional[List[TableLike]] = None,
        joins : Optional[List[Tuple[TableLike, JoinType, Optional[ExprABC]]]] = None,
        where : Optional[ExprABC] = None,
        groups: Optional[List[Column]] = None,
        orders: Optional[List[OrderedColumn]] = None,
        limit : Optional[int] = None,
        offset: Optional[int] = None,
    ) -> TableData:
        """ Run SELECT query

        Args:
            columns_or_tables (ExprABC): Columns or Tables to select.
            from_tables (Optional[List[TableLike]], optional): Tables for FROM clause. Defaults to None.
            joins (Optional[List[Tuple[TableLike, JoinType, ExprABC]]], optional): Table joins. Defaults to None.
            where (Optional[ExprABC], optional): Where expression. Defaults to None.
            groups (Optional[List[Column]], optional): Column groups. Defaults to None.
            orders (Optional[List[OrderedColumn]], optional): Column orders. Defaults to None.
            limit (Optional[int], optional): Limit value. Defaults to None.
            offset (Optional[int], optional): Offset value. Defaults to None.

        Raises:
            RuntimeError: _description_

        Returns:
            TableData: Result rows
        """

        columns_or_tables = [c for c in _columns_or_tables if c is not None]
        from_tables = [self.table(t) for t in (froms or [])]
        specified_tables: Set[Table] = set([*from_tables, *([self.table(t) for t, _, _ in joins] if joins else [])])
        used_tables = set(iter_tables(*columns_or_tables))
        from_tables.extend(used_tables - specified_tables)
        if not from_tables:
            raise errors.ObjectNotSpecifiedError('No tables specified for `from.`')

        return self.query(
            b'SELECT',
            [c.q_select() for c in columns_or_tables] if columns_or_tables else b'*',
            b'FROM', from_tables,
            *((
                make_join_type(join_type), b'JOIN',
                self.table(t),
                (b'ON', expr) if expr is not None else None
            ) for t, join_type, expr in (joins or [])),
            (b'WHERE', where) if where else None,
            (b'GROUP', b'BY', groups) if groups else None,
            (b'ORDER', b'BY', [c.q_order() for c in orders]) if orders else None,
            (b'LIMIT', limit) if limit else None,
            (b'OFFSET', offset) if offset else None,
        )


    def insert(self,
        tablelike: TableLike,
        data: Optional[Dict[ColumnLike, Any]] = None,
        **values,
    ) -> int:
        """ Run INSERT query

        Args:
            tablelike (TableLike): Table to insert
            data (Optional[Union[Dict[ColumnLike, Any], TableData]], optional): Data to insert. Defaults to None.

        Returns:
            int: _description_
        """
        table = self.table(tablelike)
        column_values = self._proc_colval_args(table, data, **values)
        self.execute(
            b'INSERT', b'INTO', table, b'(', column_values.keys(), b')',
            b'VALUES', b'(', column_values.values(),  b')',
        )
        return self.cnx.last_row_id()


    def insert_data(self,
        tablelike: TableLike,
        data: TableData,
    ) -> int:
        """ Run INSERT with TableData """
        table = self.table(tablelike)
        columns = [table.column(name) for name in data.iter_columns()]
        self.cnx.execute_with_many_prms((
            b'INSERT', b'INTO', table, b'(', columns, b')',
            b'VALUES', b'(', [b'?' for _ in columns],  b')',
        ), data)
        return self.cnx.last_row_id()


    def update(self,
        tablelike: TableLike,
        data: Optional[Dict[ColumnLike, Any]] = None,
        *,
        where: Optional[ExprABC],
        orders: Optional[List[OrderedColumn]] = None,
        limit: Optional[int] = None,
        **values,
    ) -> None:
        """ Run UPDATE query """

        table = self.table(tablelike)
        column_values = self._proc_colval_args(table, data, **values)
        self.execute(
            b'UPDATE', table, b'SET', [(c, b'=', v) for c, v in column_values.items()],
            (b'WHERE', where) if where else None,
            (b'ORDER', b'BY', [c.q_order() for c in orders]) if orders else None,
            (b'LIMIT', limit) if limit else None,
        )


    def update_data(self,
        tablelike: TableLike,
        data: TableData,
        keys: List[ColumnLike],
    ) -> None:
        """ Run UPDATE query with TableData """
        table = self.table(tablelike)

        data_columns = [c for c in data.columns if c not in keys]
        sorted_columns = [*data_columns, *keys]
        assert len(sorted_columns) == len(data.columns)
        
        self.cnx.execute_with_many_prms((
            b'UPDATE', table, b'SET', [(c, b'=', b'?') for c in data_columns],
            b'WHERE', [(c, b'==', b'?') for c in keys]
        ), data.copy_with_columns(sorted_columns))


    def delete(self,
        tablelike: TableLike,
        *,
        where: Optional[ExprABC],
        orders: Optional[List[OrderedColumn]] = None,
        limit: Optional[int] = None,
    ) -> None:
        """ Run DELETE query """
        table = self.table(tablelike)
        self.execute(
            b'DELETE', b'FROM', table,
            (b'WHERE', where) if where else None,
            (b'ORDER', b'BY', [c.q_order() for c in orders]) if orders else None,
            (b'LIMIT', limit) if limit else None,
        )


    def _proc_colval_args(self,
        table: Table, 
        value_dict: Optional[Dict[ColumnLike, Any]],
        **values
    ):
        return {table.column(c): v for c, v in [*(value_dict.items() if value_dict else []), values.items()]}
