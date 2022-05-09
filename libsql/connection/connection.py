"""
    SQL Connection classes and functions
"""
from abc import abstractmethod
from typing import Any, Collection, Dict, Iterable, Iterator, List, Optional, Set, Tuple, Union

from libsql.syntax.query_data import QueryData

from ..utils.tabledata import TableData
from ..schema.database import Database
from ..syntax.sql_values import SQLValue

class ConnectionABC:
    """ Database connection ABC """

    def __init__(self, *args, database: str, dynamic=False, **kwargs) -> None:
        self._cnx_args = args
        self._cnx_kwargs = {'database': database, **kwargs}
        self._dbname = database.encode()
        self._last_qd: Optional[QueryData] = None
        self._db_dynamic = dynamic
        self._db: Optional[Database] = None

    def initialize_database(self) -> None:
        self._db = Database(self._dbname, cnx=self, dynamic=self._db_dynamic)

    @property
    def db(self) -> Database:
        if self._db is None:
            raise RuntimeError('Database is not initialized.')
        return self._db

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
    def execute_with_stmt_prms(self, stmt: bytes, params: Optional[Collection[SQLValue]] = None) -> None:
        """ Execute a query.

        Args:
            stmt (bytes): SQL Statement to query
            params_list (Optional[List[list]], optional): List of parameter list 
                in one execution. Defaults to None.
        """

    @abstractmethod
    def execute_with_stmt_many_prms(self, stmt: bytes, params_list: Iterable[Collection[SQLValue]]) -> None:
        """ Execute a query with multiple lists of parameters.

        Args:
            stmt (bytes): SQL Statement to query
            params_list (Iterable[list]): List of parameter list 
                in one execution. Defaults to None.
        """

    @abstractmethod
    def query_with_stmt_prms(self, stmt: bytes, params: Optional[Collection[SQLValue]] = None) -> TableData:
        """ Execute a query and get result.

        Args:
            stmt (bytes): SQL Statement to query
            params_list (Optional[List[list]], optional): List of parameter list 
                in one execution. Defaults to None.

        Returns:
            TableData: Fetched data (if exists)
        """

    @abstractmethod
    def query_with_stmt_many_prms(self, stmt: bytes, params_list: Iterable[Collection[SQLValue]]) -> Iterator[TableData]:
        """ Execute a query with multiple lists of parameters, and get results.

        Args:
            stmt (bytes): SQL Statement to query
            params_list (Iterable[list]): List of parameter list in one execution. 

        Returns:
            Iterator[TableData]: Fetched data (if exists)
        """

    def execute_qd(self, qd: QueryData) -> None:
        self._last_qd = qd = qd
        return self.execute_with_stmt_prms(qd.stmt, qd.prms)

    def execute(self, *args, **kwargs) -> None:
        return self.execute_qd(QueryData(*args, **kwargs))

    def execute_with_prms(self, stmt, prms) -> None:
        qd = QueryData(stmt)
        assert not qd.prms
        return self.execute_with_stmt_prms(qd.stmt, prms)

    def execute_with_many_prms(self, stmt, prms_list) -> None:
        qd = QueryData(stmt)
        assert not qd.prms
        return self.execute_with_stmt_many_prms(qd.stmt, prms_list)

    def query_qd(self, qd: QueryData) -> TableData:
        self._last_qd = qd = qd
        return self.query_with_stmt_prms(qd.stmt, qd.prms)

    def query(self, *args, **kwargs) -> TableData:
        return self.query_qd(QueryData(*args, **kwargs))

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

    @property
    def last_qd(self) -> Optional[QueryData]:
        return self._last_qd
