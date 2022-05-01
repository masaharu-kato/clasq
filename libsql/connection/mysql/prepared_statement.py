"""
"""
from typing import List, Optional, Tuple

from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.constants import ServerFlag

from ...utils.tabledata import TableData
from ..common.prepared_connection import PreparedStatementExecutorABC

MAX_RESULTS = 4294967295

class MySQLPreparedStatementExecutor(PreparedStatementExecutorABC):

    def __init__(self, cnx: MySQLConnectionAbstract, stmt: bytes):
        self._cnx = cnx # Prepare before super init
        super().__init__(stmt)
    
    def _new(self) -> Tuple[int, int]:
        """ Create a new prepared statement (Override)
            Return: stmt_id, number of params
        """
        res = self._cnx.cmd_stmt_prepare(self._stmt)
        return res['statement_id'], res['num_params']

    def _send_params(self, params: list):
        """ Execute a specific prepared statement """
        return self._cnx.cmd_stmt_execute(
            self._stmt_id,
            params,
            ['?' for _ in params],
        )

    def reset(self) -> None:
        """ Close a specific prepared statement (Override) """
        return self._cnx.cmd_stmt_reset(self._stmt_id)

    def close(self) -> None:
        """ Close a specific prepared statement (Override) """
        return self._cnx.cmd_stmt_close(self._stmt_id)

    def _send_params_and_get_data(self, params: list) -> TableData:

        res = self._send_params(params)
        if not isinstance(res, list):
            raise RuntimeError('Invalid result type.')
        
        column_desc: List[tuple] = res[1]
        self.cnx.unread_result = True
        flags = res[2]['status_flag']
        _cursor_exists = flags & ServerFlag.STATUS_CURSOR_EXISTS != 0

        if _cursor_exists:
            self.cnx.cmd_stmt_fetch(self.stmt_id, MAX_RESULTS)

        rows, eof = self.cnx.get_rows(binary=True, columns=column_desc)
        
        column_names = List[str] = [str(c[0]) for c in column_desc]
        return TableData(column_names, rows)
