"""
    SQL Connection classes and functions
"""
from abc import abstractmethod, abstractproperty
from typing import List, Optional, Sequence, Union
import warnings

import mysql.connector # type: ignore
import mysql.connector.errors # type: ignore
import mysql.connector.abstracts
from mysql.connector.pooling import MySQLConnectionPool # type: ignore

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

class MySQLConnectionABC(ConnectionABC):

    def __init__(self, *args, db_schema:DBSchema, dictionary:bool=False, named_tuple:bool=False, **kwargs):
        super().__init__(db_schema=db_schema)
        self.cnx_maker   = lambda: self.new_cnx(*args, **kwargs)
        self.cnx         = self.cnx_maker()
        self.cursor_dict = dictionary
        self.cursor_ntpl = named_tuple

    @abstractmethod
    def new_cnx(self, *args, **kwargs):
        """ Create a new connection """
        raise NotImplementedError()

    def commit(self):
        """ Commit the current transaction """
        assert self.cnx is not None
        return self.cnx.commit()

    def create_cursor(self, *args, **options) -> 'MySQLCursor':
        """ Get cursor object """
        if self.cursor_dict:
            options['dictionary'] = True
        if self.cursor_ntpl:
            options['named_tuple'] = True
        try:
            return MySQLCursor(self, self.cnx.cursor(*args, **options))
        except (AttributeError, mysql.connector.errors.Error) as e:
            warnings.warn(str(e))
            self.close()
            self.cnx = self.cnx_maker()
            return MySQLCursor(self, self.cnx.cursor(*args, **options))

    def close(self) -> None:
        """ Close the cursor """
        if self.cnx is not None:
            try:
                self.cnx.close()
            except mysql.connector.errors.OperationalError as e:
                warnings.warn(str(e))
            self.cnx = None

    def __exit__(self, ex_type, ex_value, trace):
        self.close()

class MySQLConnection(MySQLConnectionABC):
    """ MySQL Connection Class """

    def new_cnx(self, *args, **kwargs):
        return mysql.connector.connect(*args, **kwargs) # MySQL connection 

class MySQLPooledConnection(MySQLConnectionABC):
    """ MySQL Connection from connection-pool """

    def __init__(self, *args,
        db_schema: DBSchema, dictionary: bool = False, named_tuple: bool = False,
        pool_size:int = 16,
        **kwargs):
        print('MySQLPooledConnection: Init with pool_size=%d' % pool_size)
        self.cnx_pool = MySQLConnectionPool(pool_size=pool_size, *args, **kwargs)
        super().__init__(*args, db_schema=db_schema, dictionary=dictionary, named_tuple=named_tuple, **kwargs)

    def new_cnx(self, *args, **kwargs):
        return self.cnx_pool.get_connection()

    def create_cursor(self, *args, **options) -> 'MySQLCursor':
        self.close()
        self.cnx = self.new_cnx()
        return MySQLCursor(self, self.cnx.cursor(*args, **self._cursor_options(**options)))

    # def close(self) -> None:
    #     """ Close cursor """
        # if self.cnx is not None:
        #     self.cnx.close()
        #     self.cnx = None


class MySQLCursor(CursorABC):
    """ Basic MySQL Cursor (with parent `MySQLConnection` instance) """
    def __init__(self, con:MySQLConnectionABC, cursor:MySQLCursorABC):
        print('[Debug] MySQLCursor: Init. ')
        self._con = con
        self.cursor = cursor

    @property
    def con(self):
        """ Return the connection instance """
        return self._con

    def last_row_id(self):
        """ Returns the value generated for an AUTO_INCREMENT column """
        return self.cursor.lastrowid

    def execute(self, sql:str, params:Optional[Union[list, tuple]]=()):
        """ Executes the given operation """
        return self.cursor.execute(sql, params)

    def executemany(self, sql:str, seq_params:Sequence[Union[list, tuple]]):
        """ Execute the given operation multiple times """
        return self.cursor.executemany(sql, seq_params)

        """ Returns next row of a query result set """

    def fetchall(self) -> list:
        """ Returns list of all result rows """
        return self.cursor.fetchall()

    def close(self):
        """ Close the cursor """
        try:
            return self.cursor.close()
        except (mysql.connector.errors.InternalError, ReferenceError):
            pass
