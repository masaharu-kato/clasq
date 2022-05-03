"""
    Mysql Connection class implementation
"""
from abc import abstractmethod
from typing import Dict, Iterable, Iterator, Optional

import mysql.connector # type: ignore
from mysql.connector.abstracts import MySQLConnectionAbstract # type: ignore
# from mysql.connector.pooling import MySQLConnectionPool # type: ignore

from ...utils.tabledata import TableData
from ..prepared_stmt import ConnectionABC, PreparedStatementExecutorABC
from .prepared_stmt import MySQLPreparedStatementExecutor


def connect(*args, **kwargs):
    cnx = MySQLConnection(*args, **kwargs)
    return cnx.db
    

class MySQLConnectionABC(ConnectionABC):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(dbname=str(kwargs.get('database', '')).encode())
        self.cnx_args = args
        self.cnx_kwargs = kwargs
        self.cnx: MySQLConnectionAbstract = self.new_cnx()
        self._prepared_stmts: Dict[bytes, PreparedStatementExecutorABC] = {}

    @abstractmethod
    def new_cnx(self) -> MySQLConnectionAbstract:
        """ Create a new connection """

    def commit(self):
        """ Commit the current transaction (Override) """
        return self.cnx.commit()

    ### =========================================================================================================== ###
    #    Execute and Query
    ### =========================================================================================================== ###

    def execute_plain(self, stmt: bytes) -> None:
        """ Execute a query using prepared statement """
        self.cnx.cmd_query(stmt)

    def query_plain(self, stmt: bytes) -> TableData:
        """ Execute a query using prepared statement, and get result """
        qres = self.cnx.cmd_query(stmt)
        rows, eof = self.cnx.get_rows()
        column_names = [c[0] for c in qres['columns']]
        return TableData(column_names, rows)

    def execute_with_stmt_prms(self, stmt: bytes, params: Optional[list] = None) -> None:
        """ Execute a query using prepared statement """
        if not params:
            return self.execute_plain(stmt)
        self._get_or_make_pstmt(stmt).execute_params(params)

    def execute_with_stmt_many_prms(self, stmt: bytes, params_list: Iterable[list]) -> None:
        """ Execute a query using prepared statement """
        pstmt = self._get_or_make_pstmt(stmt)
        for params in params_list:
            pstmt.execute_params(params)

    def query_with_stmt_prms(self, stmt: bytes, params: Optional[list] = None) -> TableData:
        """ Execute a query using prepared statement, and get result """
        if not params:
            return self.query_plain(stmt)
        return self._get_or_make_pstmt(stmt).query_params(params)
    
    def query_with_stmt_many_prms(self, stmt: bytes, params_list: Iterable[list]) -> Iterator[TableData]:
        """ Execute a query using prepared statement, and get result """
        pstmt = self._get_or_make_pstmt(stmt)
        for params in params_list:
            yield pstmt.query_params(params)

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
            pstmt = self._prepared_stmts[stmt] = MySQLPreparedStatementExecutor(self.cnx, stmt)
        return pstmt


class MySQLConnection(MySQLConnectionABC):
    """ MySQL Connection Class """

    def new_cnx(self):
        # TODO: Implementation with Cext of MySQL Connection
        return mysql.connector.connect(*self.cnx_args, **self.cnx_kwargs, use_pure=True) # MySQL connection 


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
