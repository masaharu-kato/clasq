"""
    SQL Connection classes and functions
"""
from abc import abstractmethod
from typing import Iterable, Iterator, List, Optional

from ..utils.tabledata import TableData


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
        """ Commit the current transaction """
