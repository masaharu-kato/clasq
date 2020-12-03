"""
    Original database cursor classes for debugging
"""
from abc import abstractmethod
from typing import Dict, List, Tuple, IO, Optional
import subprocess
import mysql.connector

CursorABC = mysql.connector.abstracts.MySQLCursorAbstract

class ExecOnlyCursor(CursorABC):

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

    def __init__(self, dbcmd:List[str], *, debug_dump:Optional[IO]=None):
        self.dbcmd = dbcmd
        self.execs:List[Tuple[str, list, bool]] = []
        self.debug_dump = debug_dump

    def execute(self, operation, params=(), multi=False):
        self.execs.append((operation, params, multi))

    def executemany(self, operation, seq_params):
        self.execs.append((operation, seq_params, True))

    def run(self) -> None:
        dump_sql = self.dumps()
        if dump_sql:
            if self.debug_dump is None:
                subprocess.run(self.dbcmd, input=dump_sql.encode())
            else:
                print(dump_sql, file=self.debug_dump)

    def dump(self, f:IO) -> None:
        for execargs in self.execs:
            print(self.format_exec(execargs), file=f)

    def dumps(self) -> str:
        return '\n'.join(map(self.format_execargs, self.execs))

    def clear(self) -> None:
        self.execs = []

    def clears(self) -> List[Tuple[str, list, bool]]:
        execs = self.execs
        self.clear()
        return execs

    def __enter__(self) -> 'SQLCommandRunner':
        return self

    def close(self):
        return self.run()

    def __exit__(self, ex_type, ex_value, trace) -> None:
        return self.close()

    @classmethod
    def format_execargs(cls, execargs:Tuple[str, list, bool]) -> str:
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
        if not _sql:
            return _sql
        sql = str(_sql).strip()
        if sql[-1] != ';':
            sql += ';'
        return sql 
