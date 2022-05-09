"""
    Prepared statement executor abstract class
"""
from abc import abstractmethod
from typing import Collection, Optional, Tuple

from ..syntax.sql_values import SQLValue
from ..utils.tabledata import TableData
from .connection import ConnectionABC
from . import errors


class PreparedStatementExecutorABC:

    def __init__(self, stmt: bytes):
        self._stmt = stmt
        self._stmt_id, self.n_params = self._new()

    @abstractmethod
    def _new(self) -> Tuple[int, int]:
        """ Create a new prepared statement
            Return: stmt_id, number of params
        """

    @abstractmethod
    def _send_params(self, params: Collection[SQLValue]) -> Optional[TableData]:
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
        except errors.ProgrammingError: # TODO: Check
            self._stmt_id = self._new()

    def run_with_params(self, params: Collection[SQLValue]) -> Optional[TableData]:
        self.reset_or_new()
        if not len(params) == self.n_params:
            raise errors.PreparedStatementPrametersError('Incorrect number of arguments for prepared statements.', self._stmt, len(params), self.n_params)
        return self._send_params(params)

    def __del__(self) -> None:
        self.close()


class PreparedConnectionABC(ConnectionABC):
    """ Database connection ABC
        (with prepared statement)
    """
