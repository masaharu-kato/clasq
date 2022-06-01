"""
"""
from typing import TYPE_CHECKING, Collection, List, Optional, Tuple

from mysql.connector.constants import ServerFlag # type: ignore

from ...syntax.sql_values import SQLValue
from ...utils.tabledata import TableData
from ..prepared_stmt import PreparedStatementExecutorABC

if TYPE_CHECKING:
    from .connection import MySQLConnectionABC

MAX_RESULTS = 4294967295

class MySQLPreparedStatementExecutor(PreparedStatementExecutorABC):

    def __init__(self, con: 'MySQLConnectionABC', stmt: bytes):
        self._con = con # Prepare before super init
        super().__init__(stmt)

    @property
    def cnx(self):
        return self._con.cnx
    
    def _new(self) -> Tuple[int, int]:
        """ Create a new prepared statement (Override)
            Return: stmt_id, number of params
        """
        res = self.cnx.cmd_stmt_prepare(self._stmt)
        return res['statement_id'], res['num_params']

    def reset(self) -> None:
        """ Close a specific prepared statement (Override) """
        return self.cnx.cmd_stmt_reset(self._stmt_id)

    def close(self) -> None:
        """ Close a specific prepared statement (Override) """
        if hasattr(self, '_stmt_id') and self.cnx.is_connected():
            self.cnx.cmd_stmt_close(self._stmt_id)

    def _send_params(self, params: Collection[SQLValue]) -> Optional[TableData]:
        """ Execute a specific prepared statement
            (Override from `PreparedStatementExecutorABC`)
        """
        res = self.cnx.cmd_stmt_execute(
            self._stmt_id,
            params,
            ['?' for _ in params],
        )
        
        if not isinstance(res[1], list):
            return None # No results
        
        column_desc: List[tuple] = res[1]
        self.cnx.unread_result = True
        flags = res[2]['status_flag']
        _cursor_exists = flags & ServerFlag.STATUS_CURSOR_EXISTS != 0

        if _cursor_exists:
            raise RuntimeError('Prepared statement cursor exists.')
            # self.cnx.cmd_stmt_fetch(self._stmt_id, MAX_RESULTS)

        rows, eof = self.cnx.get_rows(binary=True, columns=column_desc)
        
        column_names :List[str] = [str(c[0]) for c in column_desc]
        return TableData(column_names, rows)

