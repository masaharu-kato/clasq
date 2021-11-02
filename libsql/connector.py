"""
    SQL Connection classes and functions
"""
from abc import abstractmethod, abstractproperty
from typing import List, Optional, Sequence, Union
import mysql.connector # type: ignore
import mysql.connector.errors # type: ignore
import mysql.connector.abstracts # type: ignore
from .schema import Database as DBSchema


class ConnectionABC:
    """ Database connection ABC """
    def __init__(self, *, db_schema:DBSchema):
        self._db_schema = db_schema

    @abstractmethod
    def commit(self) -> None:
        """ Commit the current transaction """

    @abstractmethod
    def close(self) -> None:
        """ Close the current connection """
    
    @property
    def db(self) -> 'DBSchema':
        """ Get the database schema """
        return self._db_schema

class CursorABC:
    """ Database cursor ABC """
    
    @abstractproperty
    def con(self) -> 'ConnectionABC':
        """ Get the parent connection """
    
    @property
    def db(self) -> 'DBSchema':
        return self.con.db

    @abstractmethod
    def last_row_id(self):
        """ Get last inserted row ID """

    @abstractmethod
    def execute(self, sql:str, params:Optional[Union[list, tuple]]=()):
        """ Execute """

    @abstractmethod
    def executemany(self, sql:str, seq_params:Sequence[Union[list, tuple]]):
        """ Execute many """

    @abstractmethod
    def fetch(self):
        """ Fetch next result """

    @abstractmethod
    def fetchall(self) -> list:
        """ Fetch all results """

    @abstractmethod
    def close(self):
        """ Close this cursor """


class MySQLCursorABC(mysql.connector.abstracts.MySQLCursorAbstract):
    """ MySQLCursorABC """

class MySQLConnection(ConnectionABC):
    """ MySQL Connection Class """

    def __init__(self, *args, dictionary:bool=False, named_tuple:bool=False, db_schema:DBSchema, **kwargs):
        super().__init__(db_schema=db_schema)
        self.cnx         = mysql.connector.connect(*args, **kwargs) # MySQL connection 
        self.cursor_dict = dictionary
        self.cursor_ntpl = named_tuple

    def commit(self):
        """ Commit the current transaction """
        return self.cnx.commit()

    def create_cursor(self, *args, **kwargs) -> 'MySQLCursor':
        """ Get cursor object """
        if self.cursor_dict:
            kwargs['dictionary'] = True
        if self.cursor_ntpl:
            kwargs['named_tuple'] = True

        return MySQLCursor(self, self.cnx.cursor(*args, **kwargs))

    def close(self) -> None:
        """ Close cursor """
        if self.cnx is not None:
            self.cnx.close()
            self.cnx = None

    def __exit__(self, ex_type, ex_value, trace):
        self.close()


class MySQLCursor(CursorABC):
    """ Basic MySQL Cursor (with parent `MySQLConnection` instance) """
    def __init__(self, con:MySQLConnection, cursor:MySQLCursorABC):
        self._con = con
        self.cursor = cursor

    @property
    def con(self):
        return self._con

    def last_row_id(self):
        return self.cursor.lastrowid

    def execute(self, sql:str, params:Optional[Union[list, tuple]]=()):
        return self.cursor.execute(sql, params)

    def executemany(self, sql:str, seq_params:Sequence[Union[list, tuple]]):
        return self.cursor.executemany(sql, seq_params)

    def fetch(self):
        return self.cursor.fetch()

    def fetchall(self) -> list:
        return self.cursor.fetchall()

    def close(self):
        try:
            return self.cursor.close()
        except (mysql.connector.errors.InternalError, ReferenceError):
            pass
