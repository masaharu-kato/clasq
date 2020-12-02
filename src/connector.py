"""
    SQL Connection classes and functions
"""
from typing import Optional
from abc import ABCMeta, abstractmethod
import mysql.connector
from .executor import SQLExecutor

class DBConnectionABC(metaclass=ABCMeta):
    """ SQL Connection Abstract Class """

    @abstractmethod
    def raw_cursor(self):
        raise NotImplementedError()

    def cursor(self) -> mysql.connector.cursor.MySQLCursor:
        return SQLExecutor(self.raw_cursor())


class MySQLConnection(DBConnectionABC):
    """ MySQL Connection Class """

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.cnx = mysql.connector.MySQLConnection(*args, **kwargs)

    def raw_cursor(self):
        return self.cnx.cursor(named_tuple=True)

    def close(self) -> None:
        if self.cnx is not None:
            self.cnx.close()
            self.cnx = None

    def __exit__(self, ex_type, ex_value, trace):
        self.close()


class DebugDBCursor():
    """ Virtual Database Cursor for Debugging """

    def __init__(self, *args, **kwargs):
        self.logs = []
        self.closed = False

    def close(self) -> None:
        self._check_closed()
        self.closed = True

    def _check_closed(self) -> None:
        if self.closed:
            raise RuntimeError('Cursor already closed.')

    def execute(self, sql, params) -> None:
        self._check_closed()
        self.logs.append(('s', sql, params))

    def executemany(self, sql, params) -> None:
        self._check_closed()
        self.logs.append(('m', sql, params))

    def fetch(self) -> None:
        return None

    def fetchall(self) -> list:
        return []

    def lastrowid(self) -> int:
        return 0

    def lastlog(self) -> Optional[tuple]:
        if not self.logs:
            return None
        return self.logs[-1]


class DebugSQLConnection(DBConnectionABC):
    """ Virtual Database Connection for Debugging """

    def raw_cursor(self):
        return DebugDBCursor()

