"""
    Prepared statement executor abstract class
"""
from abc import abstractclassmethod, abstractmethod
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from ...utils.tabledata import TableData
from .connection import ConnectionABC
from .. import errors


class PreparedStatementExecutorABC:

    def __init__(self, stmt: bytes):
        self._stmt = stmt
        self._stmt_id, self.n_params = self._new(stmt)

    @abstractmethod
    def _new(cls) -> Tuple[int, int]:
        """ Create a new prepared statement
            Return: stmt_id, number of params
        """

    @abstractmethod
    def _send_params(self, params: list) -> None:
        """ Execute a specific prepared statement """

    @abstractmethod
    def _send_params_and_get_data(self, params: list) -> TableData:
        """ Execute a specific prepared statement """

    @abstractmethod
    def reset(self) -> None:
        """ Close a specific prepared statement """

    @abstractmethod
    def close(self) -> None:
        """ Close a specific prepared statement """

    def reset_or_new(self):
        # Make self prepared statement available
        try:
            self.reset()
        except errors.ProgrammingError:
            self._stmt_id = self._new(self._stmt)

    def _execute_params(self, params: list):
        self.reset_or_new()
        if not len(params) == self.n_params:
            raise errors.ProgrammingError('Incorrect number of arguments for prepared statements.')
        return self._send_params(params)

    def execute_params(self, params: list) -> None:
        self._execute_params(params)

    def query_params(self, params: list) -> TableData:
        return self._send_params_and_get_data(params)


    def __del__(self) -> None:
        self.close()


class PreparedConnectionABC(ConnectionABC):
    """ Database connection ABC
        (with prepared statement)
    """