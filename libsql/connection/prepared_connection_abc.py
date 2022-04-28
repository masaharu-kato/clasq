"""
    SQL Connection classes and functions
"""
from abc import abstractclassmethod, abstractmethod
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from ..utils.tabledata import TableData
from .connection_abc import ConnectionABC
from . import errors


class PreparedStatementABC:

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
    def _send_params_list(self, params: List[list]) -> None:
        """ Execute a specific prepared statement """

    @abstractmethod
    def _reset(self) -> None:
        """ Close a specific prepared statement """

    @abstractmethod
    def close(self) -> None:
        """ Close a specific prepared statement """

    def reset(self):
        # Make self prepared statement available
        try:
            self._reset()
        except errors.ProgrammingError:
            self._stmt_id = self._new(self._stmt)

    def _execute_params(self, params: list):
        self.reset()
        if not len(params) == self.n_params:
            raise errors.ProgrammingError('Incorrect number of arguments for prepared statements.')
        return self._send_params(params)

    def execute_params(self, params: list) -> None:
        self._execute_params(params)

    def query_params(self, params: list) -> TableData:
        res = self._execute_params(params)
        if not isinstance(res, list):
            raise RuntimeError('Invalid result type.')
        
        desc = res[1]
        self.cnx.unread_result = True
        flags = res[2]['status_flag']
        _cursor_exists = flags & ServerFlag.STATUS_CURSOR_EXISTS != 0

        if _cursor_exists:
            self.cnx.cmd_stmt_fetch(self.stmt_id, 99)
        tmp, eof = self.cnx.get_rows(binary=True, columns=desc)
        ...


    def __del__(self) -> None:
        self.close()


class PreparedConnectionABC(ConnectionABC):
    """ Database connection ABC
        (with prepared statement)
    """