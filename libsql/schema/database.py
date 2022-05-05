"""
    Database class definition
"""
from typing import TYPE_CHECKING, Any, Dict, Optional, List, Tuple, Set, Union

from libsql.syntax.query_data import QueryData

from ..syntax.exprs import ExprABC, ObjectABC, Object, NamedExprABC, Name
from ..syntax.keywords import JoinType
from ..syntax import errors
from .table import Table, iter_tables
from .column import Column, NamedExpr, OrderedColumn
from ..utils.tabledata import TableData

if TYPE_CHECKING:
    from ..connection import ConnectionABC
    

class Database(Object):
    """ Database Expr """

    def __init__(self,
        name: Name,
        *tables: Table,
        cnx: Optional['ConnectionABC'] = None,
        charset: Optional[Name] = None,
        collate: Optional[Name] = None,
        fetch_from_db: Optional[bool] = None,
        dynamic: bool = False,
        **options
    ):
        super().__init__(name)
        self._table_dict: Dict[bytes, Table] = {}
        self._cnx = cnx
        self._charset = charset
        self._collate = collate
        self._exists = bool(cnx)
        self._dynamic = dynamic
        self._options = options


        if fetch_from_db is True and tables:
            raise errors.ObjectArgsError('Tables are ignored when fetch_from_db is True')
        if fetch_from_db is not False and not tables: 
            self.fetch_from_db()
        else:
            for table in tables:
                self.append_table_object(table)

    @property
    def cnx(self):
        if self._cnx is None:
            raise errors.ObjectNotSetError('Connection is not set.')
        return self._cnx

    @property
    def charset(self):
        return self._charset

    @property
    def collate(self):
        return self._collate

    @property
    def options(self):
        return self._options

    @property
    def database(self) -> 'Database':
        return self

    @property
    def exists(self):
        return self._exists

    @property
    def is_dynamic(self):
        return self._dynamic

    def __repr__(self):
        return 'DB(%s)' % str(self)

    def iter_tables(self):
        return iter(self._table_dict.values())

    def tables(self):
        return list(self.iter_tables())

    def table(self, val: Union[Name, Table]) -> Table:
        """ Get a Table object with the specified name

        Args:
            val (bytes | str | Table): Table name or table object

        Raises:
            errors.ObjectNotFoundError: _description_
            errors.NotaSelfObjectError: _description_
            errors.ObjectArgsError: _description_

        Returns:
            Table: Table object with the specified name or Table object itself
        """
        if isinstance(val, (bytes, str)):
            name = val.encode() if isinstance(val, str) else val
            if name not in self._table_dict:
                if self.is_dynamic:
                    return self.append_table(name, dynamic=True)
                else:
                    raise errors.ObjectNotFoundError('Undefined table name `%s` on database `%s`' % (str(name), str(self._name)))
            return self._table_dict[name] 

        if isinstance(val, Table):
            if val.database == self:
                return val
            raise errors.NotaSelfObjectError('Not a table of this database.')

        raise errors.ObjectArgsError('Invalid type %s (%s)' % (type(val), val))
        
    def __getitem__(self, val: Union[Name, Table]):
        return self.table(val)

    def table_or_none(self, val: Union[Name, Table]) -> Optional[Table]:
        """ Get a Table object with the specified name if exists """
        try:
            return self.table(val)
        except (errors.ObjectNotFoundError, errors.NotaSelfObjectError):
            pass
        return None

    def get(self, val: Union[Name, Table]) -> Optional[Table]:
        """ Synonym of `table_or_none` method """
        return self.table_or_none(val)

    def append_table_object(self, table: Table) -> None:
        """ Append (existing) Table object to this Database

        Args:
            table (Table): Table object

        Raises:
            errors.NotaSelfObjectError: The Table object in the different database was specified
        """
        if table.database_or_none:
            if not table.database == self:
                raise errors.NotaSelfObjectError('Table of the different database.')
        else:
            table.set_database(self)
        self._table_dict[table.name] = table

    def append_table(self, name: Name, **options) -> 'Table':
        """ Append new table to this Database

        Args:
            name (bytes | str): Table name
            options: Table options

        Returns:
            Table: Appended new Table object
        """
        table = Table(name, database=self, **options)
        self._table_dict[table.name] = table
        return table

    def fetch_from_db(self) -> None:
        """ Fetch tables of this database from the connection """
        for tabledata in self.query(b'SHOW', b'TABLES'):
            self.append_table(tabledata[0])

    def q_create(self, *, if_not_exists=False) -> tuple:
        return (
            b'CREATE', b'DATABASE',
            (b'IF', b'NOT', b'EXISTS') if if_not_exists else None,
            self,
            (b'CHARACTER', b'SET', Object(self._charset)) if self._charset else None,
            (b'COLLATE', Object(self._collate)) if self._collate else None,
        )

    def drop(self, *, if_exists=False) -> None:
        """ Run DROP DATABASE query """
        if_exists = if_exists or (not self._exists)
        self.execute(
            b'DROP', b'DATABASE',
            (b'IF', b'EXISTS') if if_exists else None, self)
        self._exists_on_db = False

    def create(self, *, if_not_exists=False, drop_if_exists=False) -> None:
        """ Create this database """
        if drop_if_exists:
            self.drop(if_exists=True)
        self.execute(*self.q_create(if_not_exists=if_not_exists))
        self._exists_on_db = True

    @property
    def last_qd(self):
        return self.cnx.last_qd

    def execute_qd(self, qd: QueryData) -> None:
        return self.cnx.execute_qd(qd)

    def execute(self, *args, **kwargs) -> None:
        return self.cnx.execute(*args, **kwargs)

    def query_qd(self, qd: QueryData) -> TableData:
        return self.cnx.query_qd(qd)

    def query(self, *args, **kwargs) -> TableData:
        return self.cnx.query(*args, **kwargs)

    def commit(self) -> None:
        return self.cnx.commit()

    def select_query(self,
        *_columns_or_tables: Optional[Union[Table, NamedExprABC]],
        froms : Optional[List[Union[Name, Table]]] = None,
        joins : Optional[List[Tuple[Union[Name, Table], JoinType, Optional[ExprABC]]]] = None,
        where : Optional[ExprABC] = None,
        groups: Optional[List[NamedExprABC]] = None,
        orders: Optional[List[NamedExprABC]] = None,
        limit : Optional[int] = None,
        offset: Optional[int] = None,
    ) -> QueryData:
        """ Make a SELECT query data

        Returns:
            QueryData: SELECT query data
        """

        columns_or_tables = [c for c in _columns_or_tables if c is not None]
        from_tables = [self.table(t) for t in (froms or [])]
        specified_tables: Set[Table] = set([*from_tables, *([self.table(t) for t, _, _ in joins] if joins else [])])
        used_tables = set(iter_tables(*columns_or_tables))
        from_tables.extend(used_tables - specified_tables)
        if not from_tables:
            raise errors.ObjectNotSpecifiedError('No tables specified for `from.`')

        return QueryData(
            b'SELECT',
            [c.query_for_select_column() for c in columns_or_tables] if columns_or_tables else b'*',
            b'FROM', from_tables,
            *((
                JoinType.make(join_type), b'JOIN',
                self.table(t),
                (b'ON', expr) if expr is not None else None
            ) for t, join_type, expr in (joins or [])),
            (b'WHERE', where) if where else None,
            (b'GROUP', b'BY', groups) if groups else None,
            (b'ORDER', b'BY', [c.q_order() for c in orders]) if orders else None,
            (b'LIMIT', limit) if limit else None,
            (b'OFFSET', offset) if offset else None,
        )

    def select(self,
        *_columns_or_tables: Optional[NamedExprABC],
        froms : Optional[List[Union[Name, Table]]] = None,
        joins : Optional[List[Tuple[Union[Name, Table], JoinType, Optional[ExprABC]]]] = None,
        where : Optional[ExprABC] = None,
        groups: Optional[List[NamedExprABC]] = None,
        orders: Optional[List[NamedExprABC]] = None,
        limit : Optional[int] = None,
        offset: Optional[int] = None,
    ) -> TableData:
        """ Run SELECT query

        Args:
            columns_or_tables (ExprABC):
                Columns or Tables to select.

            from_tables (Optional[List[Union[Name, Table]]], optional):
                Tables for FROM clause. Defaults to None.

            joins (Optional[List[Tuple[Union[Name, Table], JoinType, ExprABC]]], optional):
                Table joins. Defaults to None.

            where (Optional[ExprABC], optional):
                Where expression. Defaults to None.

            groups (Optional[List[Column]], optional):
                Column groups. Defaults to None.

            orders (Optional[List[OrderedColumn]], optional):
                Column orders. Defaults to None.

            limit (Optional[int], optional):
                Limit value. Defaults to None.

            offset (Optional[int], optional):
                Offset value. Defaults to None.

        Returns:
            TableData: Result rows
        """
        return self.query_qd(self.select_query(
            *_columns_or_tables,
            froms  = froms,
            joins  = joins,
            where  = where,
            groups = groups,
            orders = orders,
            limit  = limit,
            offset = offset,
        ))

    def insert(self,
        tablelike: Union[Name, Table],
        data: Optional[Dict[NamedExpr, Any]] = None,
        **values,
    ) -> int:
        """ Run INSERT query

        Args:
            tablelike (Union[Name, Table]): Table to insert
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
        tablelike: Union[Name, Table],
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
        tablelike: Union[Name, Table],
        data: Optional[Dict[NamedExpr, Any]] = None,
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
        tablelike: Union[Name, Table],
        data: TableData,
        keys: List[Union[Name, Column]],
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
        tablelike: Union[Name, Table],
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
        value_dict: Optional[Dict[NamedExpr, Any]],
        **values
    ):
        return {table.column(c): v for c, v in [*(value_dict.items() if value_dict else []), values.items()]}
