"""
    SQL Connection classes and functions
"""
from typing import Optional
from abc import ABCMeta, abstractmethod
import mysql.connector
from .executor import QueryExecutor
from .schema import Database as DBSchema

MySQLCursorABC = mysql.connector.abstracts.MySQLCursorAbstract


class MySQLConnection:
    """ MySQL Connection Class """

    def __init__(self, *args, dictionary:bool=False, named_tuple:bool=False, db_schema:Optional[DBSchema]=None, **kwargs):
        super().__init__()
        self.cnx         = mysql.connector.connect(*args, **kwargs) # MySQL connection 
        self.db_schema   = db_schema   # 
        self.cursor_dict = dictionary
        self.cursor_ntpl = named_tuple

    def commit(self):
        """ Commit changes """
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


class MySQLCursor:
    """ Basic MySQL Cursor (with parent `MySQLConnection` instance) """
    def __init__(self, con:MySQLConnection, cursor:MySQLCursorABC):
        self.con = con
        self.cursor = cursor

    # def __getattr__(self, name:str):
    #     """ Call method or get property on cursor object """
    #     return getattr(self.cursor, name)
