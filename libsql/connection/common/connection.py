"""
    SQL Connection classes and functions
"""
from abc import abstractmethod
from typing import Iterable, Iterator, List, Optional, Set, Tuple, Union

from mysqlx import Column

from ...utils.tabledata import TableData
from ...syntax.expr_type import ExprType, TableExpr, ColumnExpr, OrderedColumnExpr
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
    def execute(self, stmt: bytes, params: Optional[list] = None) -> None:
        """ Execute a query.

        Args:
            stmt (bytes): SQL Statement to query
            params_list (Optional[List[list]], optional): List of parameter list 
                in one execution. Defaults to None.
        """

    @abstractmethod
    def execute_many(self, stmt: bytes, params_list: Iterable[list]) -> None:
        """ Execute a query with multiple lists of parameters.

        Args:
            stmt (bytes): SQL Statement to query
            params_list (Iterable[list]): List of parameter list 
                in one execution. Defaults to None.
        """

    @abstractmethod
    def query(self, stmt: bytes, params: Optional[list] = None) -> TableData:
        """ Execute a query and get result.

        Args:
            stmt (bytes): SQL Statement to query
            params_list (Optional[List[list]], optional): List of parameter list 
                in one execution. Defaults to None.

        Returns:
            TableData: Fetched data (if exists)
        """

    @abstractmethod
    def query_many(self, stmt: bytes, params_list: Iterable[list]) -> Iterator[TableData]:
        """ Execute a query with multiple lists of parameters, and get results.

        Args:
            stmt (bytes): SQL Statement to query
            params_list (Iterable[list]): List of parameter list in one execution. 

        Returns:
            Iterator[TableData]: Fetched data (if exists)
        """

    @abstractmethod
    def commit(self) -> None:
        """ Commit the current transaction
        """


    def select(self,
        *columns_or_tables: ExprType,
        from_tables     : Optional[List[TableExpr]] = None,
        joins           : Optional[List[Tuple[TableExpr, JoinType, ExprType]]] = None,
        where           : Optional[ExprType] = None,
        groups          : Optional[List[ColumnExpr]] = None,
        orders          : Optional[List[OrderedColumnExpr]] = None,
        limit           : Optional[int] = None,
        offset          : Optional[int] = None,
    ) -> TableData:
        """ SELECT query """

        res_prms = []
        from_tables = [*(from_tables or [])]

        all_tables: Set[TableExpr] = set(*from_tables, *([t for t, _, _ in joins] if joins else []))

        for expr in columns_or_tables:
            if isinstance(expr, TableExpr):
                if expr not in all_tables:
                    from_tables.append(expr)
            elif isinstance(expr, ColumnExpr):
                if expr.table and expr.table not in all_tables:
                    from_tables.append(expr)

        if not from_tables:
            raise RuntimeError('No tables specified for `from.`')

        res_stmt = b'SELECT '\
            + (b', '.join(c.column_expr for c in columns_or_tables) if columns_or_tables else b'*')\
            + b'\n FROM ' + b', '.join(t.name_expr for t in from_tables) + b'\n'
        
        for table, join_type, expr in joins:
            stmt, prms = expr.get_statement_data()
            res_stmt += b'%s JOIN %s ON %s\n' % (join_type.value, table.name_expr, stmt)
            res_prms.extend(prms)

        if where:
            stmt, prms = where.get_statement_data()
            res_stmt += b'WHERE %s' % stmt
            res_prms.extend(prms)

        if groups:
            res_stmt += b'GROUP BY %s\n' % b', '.join(col.column_expr for col in groups)

        if orders:
            res_stmt += b'ORDER BY %s\n' % b', '.join(col.column_expr for col in orders) # TODO: Edit

        if limit is None:
            res_stmt += b'LIMIT ?\n'
            res_prms.append(limit)

        if offset is None:
            res_stmt += b'OFFSET ?\n'
            res_prms.append(offset)

        return self.query(res_stmt, res_prms)
