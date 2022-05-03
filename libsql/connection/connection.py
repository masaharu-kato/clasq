"""
    SQL Connection classes and functions
"""
from abc import abstractmethod
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple, Union

from libsql.syntax.query_data import QueryData

from ..utils.tabledata import TableData
from ..database import Database


class ConnectionABC:
    """ Database connection ABC """

    def __init__(self, *, dbname: bytes) -> None:
        self._dbname = dbname
        self._db = Database(dbname, cnx=self) # TODO: Fetch tables
        self._last_qd: Optional[QueryData] = None

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
        self._last_qd = qd = QueryData(*args, **kwargs)
        return self.execute_with_stmt_prms(qd.stmt, qd.prms)

    def execute_with_prms(self, stmt, prms):
        return self.execute_with_stmt_prms(QueryData(stmt), prms)

    def execute_with_many_prms(self, stmt, prms_list):
        return self.execute_with_stmt_many_prms(QueryData(stmt), prms_list)

    def query(self, *args, **kwargs) -> TableData:
        self._last_qd = qd = QueryData(*args, **kwargs)
        print(qd)
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

    @property
    def last_qd(self) -> Optional[QueryData]:
        return self._last_qd

    @property
    def db(self) -> Database:
        return self._db
