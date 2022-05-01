"""
    SQL Connection classes and functions
"""
from abc import abstractmethod, abstractproperty
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple, Union

from libsql.syntax.query_data import QueryData

from ...utils.tabledata import TableData
from ...syntax.expr_type import ExprType
from ...syntax.schema_expr import DatabaseExpr, TableExpr, TableLike, ColumnExpr, ColumnLike, OrderedColumnExpr
from ...syntax.keywords import JoinType


class ConnectionABC:
    """ Database connection ABC """

    @abstractmethod
    def execute_plain(self, stmt: bytes) -> None:
        """ Execute a query (with no parameters).

        Args:
            stmt (bytes): SQL Statement to query
        """

    @abstractmethod
    def query_plain(self, stmt: bytes) -> TableData:
        """ Execute a query and get result (with no parameters).

        Args:
            stmt (bytes): SQL Statement to query

        Returns:
            TableData: Fetched data (if exists)
        """

    @abstractmethod
    def execute_with_stmt_prms(self, stmt: bytes, params: Optional[list] = None) -> None:
        """ Execute a query.

        Args:
            stmt (bytes): SQL Statement to query
            params_list (Optional[List[list]], optional): List of parameter list 
                in one execution. Defaults to None.
        """

    @abstractmethod
    def execute_with_stmt_many_prms(self, stmt: bytes, params_list: Iterable[list]) -> None:
        """ Execute a query with multiple lists of parameters.

        Args:
            stmt (bytes): SQL Statement to query
            params_list (Iterable[list]): List of parameter list 
                in one execution. Defaults to None.
        """

    @abstractmethod
    def query_with_stmt_prms(self, stmt: bytes, params: Optional[list] = None) -> TableData:
        """ Execute a query and get result.

        Args:
            stmt (bytes): SQL Statement to query
            params_list (Optional[List[list]], optional): List of parameter list 
                in one execution. Defaults to None.

        Returns:
            TableData: Fetched data (if exists)
        """

    @abstractmethod
    def query_with_stmt_many_prms(self, stmt: bytes, params_list: Iterable[list]) -> Iterator[TableData]:
        """ Execute a query with multiple lists of parameters, and get results.

        Args:
            stmt (bytes): SQL Statement to query
            params_list (Iterable[list]): List of parameter list in one execution. 

        Returns:
            Iterator[TableData]: Fetched data (if exists)
        """

    def execute(self, *args, **kwargs) -> None:
        qd = QueryData(*args, **kwargs)
        return self.execute_with_stmt_prms(qd.stmt, qd.prms)

    def execute_with_prms(self, stmt, prms):
        return self.execute_with_stmt_prms(QueryData(stmt), prms)

    def execute_with_many_prms(self, stmt, prms_list):
        return self.execute_with_stmt_many_prms(QueryData(stmt), prms_list)

    def query(self, *args, **kwargs) -> TableData:
        qd = QueryData(*args, **kwargs)
        return self.query_with_stmt_prms(qd.stmt, qd.prms)

    def query_with_prms(self, stmt, prms):
        return self.query_with_stmt_prms(QueryData(stmt), prms)

    def query_with_many_prms(self, stmt, prms_list):
        return self.query_with_stmt_many_prms(QueryData(stmt), prms_list)


    @abstractmethod
    def commit(self) -> None:
        """ Commit the current transaction
        """

    @abstractmethod
    def last_row_id(self) -> int:
        """ Get a last inserted row id """

    @abstractproperty
    def db(self) -> DatabaseExpr:
        """ Get a database expr """

    def select(self,
        *columns_or_tables: ExprType,
        from_tables     : Optional[List[TableLike]] = None,
        joins           : Optional[List[Tuple[TableLike, JoinType, ExprType]]] = None,
        where           : Optional[ExprType] = None,
        groups          : Optional[List[ColumnExpr]] = None,
        orders          : Optional[List[OrderedColumnExpr]] = None,
        limit           : Optional[int] = None,
        offset          : Optional[int] = None,
    ) -> TableData:
        """ SELECT query """

        from_tables = [*([self.db[t] for t in from_tables] or [])]
        specified_tables: Set[TableExpr] = set(*from_tables, *([t for t, _, _ in joins] if joins else []))
        used_tables = set(e for e in (e.table_expr() for e in columns_or_tables) if e is not None)
        from_tables.extend(used_tables - specified_tables)
        if not from_tables:
            raise RuntimeError('No tables specified for `from.`')

        return self.query(
            b'SELECT',
            [c.column_def_expr() for c in columns_or_tables] if columns_or_tables else b'*',
            b'FROM', from_tables,
            *((join_type, b'JOIN', self.db[table], b'ON', expr) for table, join_type, expr in joins),
            (b'WHERE', where) if where else None,
            (b'GROUP', b'BY', groups) if groups else None,
            (b'ORDER', b'BY', [c.q_order() for c in orders]) if orders else None,
            (b'LIMIT', limit) if limit else None,
            (b'OFFSET', offset) if offset else None,
        )


    def insert(self,
        tablelike: TableLike,
        data: Optional[Union[Dict[ColumnLike, Any], TableData]] = None,
        **values,
    ) -> int:
        """ INSERT query """

        if isinstance(data, TableData):
            assert not values
            return self.insert_with_data(tablelike, data)

        table = self.db[tablelike]
        column_values = self._proc_colval_args(table, data, **values)
        self.execute(
            b'INSERT', b'INTO', table, b'(', column_values.keys(), b')',
            b'VALUES', b'(', column_values.values(),  b')',
        )
        return self.last_row_id()


    def insert_with_data(self,
        tablelike: TableLike,
        data: TableData,
    ):
        """ INSERT with TableData """
        table = self.db[tablelike]
        columns = [table[name] for name in data.iter_columns()]
        self.execute_with_many_prms((
            b'INSERT', b'INTO', table, b'(', columns, b')',
            b'VALUES', b'(', [b'?' for _ in columns],  b')',
        ), data)
        return self.last_row_id()


    def update(self,
        tablelike: TableLike,
        data: Optional[Union[Dict[ColumnLike, Any], TableData]] = None,
        *,
        where: Optional[ExprType],
        orders: Optional[List[OrderedColumnExpr]] = None,
        limit: Optional[int] = None,
        **values,
    ):
        """ UPDATE query """

        if isinstance(data, TableData):
            assert not values and not orders and not limit
            return self.update_with_data(tablelike, data)

        table = self.db[tablelike]
        column_values = self._proc_colval_args(table, data, **values)
        self.execute(
            b'UPDATE', table, b'SET', [(c, b'=', v) for c, v in column_values.items()],
            (b'WHERE', where) if where else None,
            (b'ORDER', b'BY', [c.q_order() for c in orders]) if orders else None,
            (b'LIMIT', limit) if limit else None,
        )


    def update_with_data(self,
        tablelike: TableLike,
        data: TableData,
        keys: List[ColumnLike],
    ):
        """ UPDATE query with TableData """
        table = self.db[tablelike]

        data_columns = [c for c in data.columns if c not in keys]
        sorted_columns = [*data_columns, *keys]
        assert len(sorted_columns) == len(data.columns)
        
        self.execute_with_many_prms((
            b'UPDATE', table, b'SET', [(c, b'=', b'?') for c in data_columns],
            b'WHERE', [(c, b'==', b'?') for c in keys]
        ), data.copy_with_columns(sorted_columns))


    def delete(self,
        tablelike: TableLike,
        where: Optional[ExprType],
        orders: Optional[List[OrderedColumnExpr]] = None,
        limit: Optional[int] = None,
    ):
        table = self.db[tablelike]
        self.execute(
            b'DELETE', b'FROM', table,
            (b'WHERE', where) if where else None,
            (b'ORDER', b'BY', [c.q_order() for c in orders]) if orders else None,
            (b'LIMIT', limit) if limit else None,
        )


    
    def _insert_stmt(self, table: TableExpr, columns: List[ColumnExpr]):
        qd = QueryData(
            b'INSERT', b'INTO', table, b'(', columns, b')',
            b'VALUES', b'(', [b'?' for _ in columns],  b')',
        )
        assert not qd.prms
        return qd.stmt

    def _proc_colval_args(self,
        table: TableExpr, 
        value_dict: Optional[Dict[ColumnLike, Any]],
        **values
    ):
        return {table[c]: v for c, v in [*value_dict.items(), values.items()]}
