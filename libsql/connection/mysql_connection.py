"""
    Mysql Connection class implementation
"""
from abc import abstractmethod
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract
import mysql.connector.pooling

from ..utils.tabledata import TableData
from .prepared_connection_abc import ConnectionABC, PreparedConnectionABC, PreparedStatementABC

class MySQLConnectionABC(ConnectionABC):

    def __init__(self) -> None:
        super().__init__()
        self.cnx: MySQLConnectionAbstract = self.new_cnx()
        self._prepared_stmts: Dict[bytes, PreparedStatementABC] = {}

    @abstractmethod
    def new_cnx(self, *args, **kwargs) -> MySQLConnectionAbstract:
        """ Create a new connection """

    def execute_plain(self, stmt: bytes) -> None:
        """ Execute a query using prepared statement """
        self.cnx.cmd_query(stmt)

    def query_plain(self, stmt: bytes) -> TableData:
        """ Execute a query using prepared statement, and get result """
        self.cnx.cmd_query(stmt)
        return self.cnx.get_rows()

    def commit(self):
        """ Commit the current transaction (Override) """
        return self.cnx.commit()

    def execute(self, stmt: bytes, params: Optional[list] = None) -> None:
        """ Execute a query using prepared statement """
        if not params:
            return self.execute_plain(stmt)
        self._get_or_make_pstmt(stmt).execute_params(params)

    def execute_many(self, stmt: bytes, params_list: Iterable[list]) -> None:
        """ Execute a query using prepared statement """
        pstmt = self._get_or_make_pstmt(stmt)
        for params in params_list:
            pstmt.execute_params(params)

    def query(self, stmt: bytes, params: Optional[list] = None) -> TableData:
        """ Execute a query using prepared statement, and get result """
        if not params:
            return self.query_plain(stmt)
        pstmt = self._get_or_make_pstmt(stmt)
        pstmt.execute_params(params)
    
    def query_many(self, stmt: bytes, params_list: Iterable[list]) -> Iterator[TableData]:
        """ Execute a query using prepared statement, and get result """
        pstmt = self._get_or_make_pstmt(stmt)
        for params in params_list:
            pstmt.execute_params(params)

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


    def _get_or_make_pstmt(self, stmt: bytes) -> PreparedStatementABC:
        if not (pstmt := self._prepared_stmts.get(stmt)):
            pstmt = self._prepared_stmts[stmt] = MySQLPreparedStatement(self.cnx, stmt)
        return pstmt

    def _get_results_by_pstmt(self) -> TableData:
        ...


class MySQLConnection(MySQLConnectionABC):
    """ MySQL Connection Class """

    def new_cnx(self, *args, **kwargs):
        return mysql.connector.connect(*args, **kwargs) # MySQL connection 



class MySQLPreparedStatement(PreparedStatementABC):

    def __init__(self, cnx: MySQLConnectionAbstract, stmt: bytes):
        super().__init__(stmt)
        self._cnx = cnx
    
    def _new(self) -> Tuple[int, int]:
        """ Create a new prepared statement (Override)
            Return: stmt_id, number of params
        """
        res = self._cnx.cmd_stmt_prepare(self._stmt)
        return res['statement_id'], res['num_params']

    def _send_params_list(self, params_list: List[list]) -> Optional[TableData]:
        """ Execute a specific prepared statement (Override) """


    def _reset(self) -> None:
        """ Close a specific prepared statement (Override) """
        return self._cnx.cmd_stmt_reset(self._stmt_id)

    def _close(self) -> None:
        """ Close a specific prepared statement (Override) """
        return self._cnx.cmd_stmt_close(self._stmt_id)


# class MySQLPooledConnection(MySQLConnectionABC):
#     """ MySQL Connection from connection-pool """

#     def __init__(self, *args,
#         dictionary: bool = False, named_tuple: bool = False,
#         pool_size:int = 16,
#         **kwargs):
#         print('MySQLPooledConnection: Init with pool_size=%d' % pool_size)
#         self.cnx_pool = mysql.connector.pooling.MySQLConnectionPool(pool_size=pool_size, *args, **kwargs)
#         super().__init__(*args, dictionary=dictionary, named_tuple=named_tuple, **kwargs)

#     def new_cnx(self, *args, **kwargs):
#         return self.cnx_pool.get_connection()
