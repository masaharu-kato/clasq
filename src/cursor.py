"""
    Original database cursor classes for debugging
"""
from abc import abstractmethod
from typing import List, Tuple, IO, Optional
import subprocess
from . import connector

CursorABC = connector.CursorABC
MySQLConnection = connector.MySQLConnection
MySQLCursor = connector.MySQLCursor


class ExecOnlyCursor(CursorABC):
    """ Execute only cursor """

    def callproc(self, procname, args=()):
        raise NotImplementedError()

    @abstractmethod
    def close(self):
        pass

    def fetchone(self):
        raise NotImplementedError()

    def fetchmany(self, size=1):
        raise NotImplementedError()

    def fetchall(self):
        raise NotImplementedError()

    @property
    def description(self):
        raise NotImplementedError()

    @property
    def rowcount(self):
        raise NotImplementedError()

    @property
    def lastrowid(self):
        raise NotImplementedError()


class CommandCursor(ExecOnlyCursor):
    """ Virtual Database Cursor for Debugging """

    def __init__(self, dbcmd:List[str], *args, debug_dump:Optional[IO]=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.dbcmd = dbcmd
        self.execs:List[Tuple[str, list, bool]] = []
        self.debug_dump = debug_dump

    def execute(self, operation, params=(), multi=False):
        self.execs.append((operation, params, multi))

    def executemany(self, operation, seq_params):
        self.execs.append((operation, seq_params, True))

    def run(self) -> None:
        """ Run queued statements """
        dump_sql = self.dumps()
        if dump_sql:
            if self.debug_dump is None:
                subprocess.run(self.dbcmd, input=dump_sql.encode(), check=True)
            else:
                print(dump_sql, file=self.debug_dump)

    def dump(self, f:IO) -> None:
        """ Dump queued statements to file """
        for execargs in self.execs:
            print(self.format_execargs(execargs), file=f)

    def dumps(self) -> str:
        """ Get queded statements """
        return '\n'.join(map(self.format_execargs, self.execs))

    def clear(self) -> None:
        """ Clear queued statements """
        self.execs = []

    def clears(self) -> List[Tuple[str, list, bool]]:
        """ Get queued statements and clear them """
        execs = self.execs
        self.clear()
        return execs

    def __enter__(self) -> 'SQLCommandRunner':
        return self

    def close(self):
        """ Close cursor """
        self.run()
        super().close()

    def __exit__(self, ex_type, ex_value, trace) -> None:
        return self.close()

    @classmethod
    def format_execargs(cls, execargs:Tuple[str, list, bool]) -> str:
        """ Format arguments in execution """
        _sql, params, multi = execargs
        sql = cls.format_sql(_sql)
        if params:
            sql_for_fmt = sql.replace('%s', '{}')
            if not multi:
                return sql_for_fmt.format(*params)
            return '\n'.join(sql_for_fmt.format(*cparams) for cparams in params)
        return sql

    @classmethod
    def format_sql(cls, _sql:str) -> str:
        """ Format sql statement """
        if not _sql:
            return _sql
        sql = str(_sql).strip()
        if sql[-1] != ';':
            sql += ';'
        return sql 
