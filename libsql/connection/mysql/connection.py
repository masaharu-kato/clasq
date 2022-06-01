"""
    Mysql Connection class implementation
"""
from __future__ import annotations
from abc import abstractproperty
from typing import Collection, Iterable, Iterator

import mysql.connector # type: ignore
from mysql.connector.abstracts import MySQLConnectionAbstract # type: ignore

# from mysql.connector.pooling import MySQLConnectionPool # type: ignore

from ...syntax.abc.object import ObjectName, NameLike
from ...syntax.sql_values import SQLValue
from ...utils.tabledata import TableData
from ..prepared_stmt import ConnectionABC, PreparedStatementExecutorABC
from .prepared_stmt import MySQLPreparedStatementExecutor

class MySQLConnectionABC(ConnectionABC):

    def __init__(self, **cnx_options) -> None:
        self._prepared_stmts: dict[bytes, PreparedStatementExecutorABC] = {}
        super().__init__(**cnx_options)

    @abstractproperty
    def cnx(self) -> MySQLConnectionAbstract:
        """ Get a connection """

    def commit(self):
        """ Commit the current transaction (Override) """
        return self.cnx.commit()

    def _use_db(self, dbname: NameLike) -> None:
        self.cnx.database = str(ObjectName(dbname))

    def last_row_id(self) -> int:
        # TODO: Implement
        raise NotImplementedError()

    ### =========================================================================================================== ###
    #    Execute and Query
    ### =========================================================================================================== ###
    
    def run_stmt_prms(self, stmt: bytes, prms: Collection[SQLValue] = ()) -> TableData | None:
        """ Execute a query with single list of params and get result if exists
            (Override from `ConnectionABC`)
        """
        if not prms:
            return self.run_stmt(stmt)
        return self._get_or_make_pstmt(stmt).run_with_params(prms)

    def run_stmt_many_prms(self, stmt: bytes, prms_list: Iterable[Collection[SQLValue]]) -> Iterator[TableData | None]:
        """ Execute a query with multiple lists of params and get result if exists
            (Override from `ConnectionABC`)
        """ 
        pstmt = self._get_or_make_pstmt(stmt)
        for prms in prms_list:
            yield pstmt.run_with_params(prms)

    def run_stmt(self, stmt: bytes) -> TableData | None:
        """ Execute a query using prepared statement, and get result """
        qres = self.cnx.cmd_query(stmt)
        if isinstance(qres, dict) and 'columns' in qres:
            column_names = [c[0] for c in qres['columns']]
            self.cnx._unread_result = True
            rows, eof = self.cnx.get_rows()
            return TableData(column_names, rows)
        return None

    def close_all_prepared_stmts(self):
        """ Close all prepared statements """
        for stmt in list(self._prepared_stmts):
            del self._prepared_stmts[stmt]

    def __exit__(self, ex_type, ex_value, trace):
        """ Close the cursor """
        self.cnx.close()
        # if self.cnx is not None:
        #     try:
        #         self.cnx.close()
        #     except mysql.connector.errors.OperationalError as e:
        #         warnings.warn(str(e))
        #     self.cnx = None

    def _get_or_make_pstmt(self, stmt: bytes) -> PreparedStatementExecutorABC:
        if not (pstmt := self._prepared_stmts.get(stmt)):
            pstmt = self._prepared_stmts[stmt] = MySQLPreparedStatementExecutor(self, stmt)
        return pstmt


class MySQLConnection(MySQLConnectionABC):
    """ MySQL Connection Class """

    def __init__(self, **cnx_options) -> None:
        self._cnx: MySQLConnectionAbstract | None = None
        super().__init__(**cnx_options)

    @property
    def cnx(self) -> MySQLConnectionAbstract:
        """ Get a connection """
        self.connect()
        assert self._cnx is not None
        return self._cnx

    def new_cnx(self) -> MySQLConnectionAbstract:
        # TODO: Implementation with Cext of MySQL Connection
        # print('new cnx')
        return mysql.connector.connect(**self.cnx_options, use_pure=True) # MySQL connection

    def is_connected(self) -> bool:
        return self._cnx is not None # and self._cnx.is_connected()

    def connect(self) -> None:
        """ Make a connection if not connected """
        if not self.is_connected():
            self._cnx = self.new_cnx()

    def disconnect(self) -> None:
        """ Disconnect if connected """
        if self.is_connected():
            assert self._cnx is not None
            self._cnx.disconnect()

    



# class MySQLPooledConnection(MySQLConnectionABC):
#     """ MySQL Connection from connection-pool """

#     def __init__(self, *args,
#         dictionary: bool = False, named_tuple: bool = False,
#         pool_size:int = 16,
#         **kwargs):
#         print('MySQLPooledConnection: Init with pool_size=%d' % pool_size)
#         self.cnx_pool = MySQLConnectionPool(pool_size=pool_size, *args, **kwargs)
#         super().__init__(*args, dictionary=dictionary, named_tuple=named_tuple, **kwargs)

#     def new_cnx(self, *args, **kwargs):
#         return self.cnx_pool.get_connection()
