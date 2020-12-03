"""
    SQL Execution classes and functions
"""
from typing import Any, Callable, Dict, IO, Iterable, List, Optional 
import mysql.connector
from .cursor import CursorABC

_IS_DEBUG = True

def _fo(objname:str) -> str:
    if _IS_DEBUG:
        if '`' in objname:
            raise RuntimeError('Invalid character(s) found in the object name.')
    return '`' + objname + '`'


class SQLExecutor:
    """ SQL実行クラス """

    def __init__(self, cursor:CursorABC):
        """ Initialize a instance with a cursor to the Database """
        if not isinstance(cursor, CursorABC):
            raise TypeError('Invalid type of cursor.')
        if cursor is None:
            raise RuntimeError('Cursor is None.')
        self.cursor = cursor

    def __enter__(self):
        return self

    def drop_database(self, name:str) -> None:
        self.execute(f'DROP DATABASE IF EXISTS {_fo(name)}')

    def drop_user(self, name:str) -> None:
        self.execute(f'DROP USER IF EXISTS {_fo(name)}')

    def drop_table(self, name:str) -> None:
        self.execute(f'DROP TABLE IF EXISTS {_fo(name)}')

    def create_database(self, name:str) -> None:
        self.execute(f'CREATE DATABASE {_fo(name)}')

    def use_database(self, name:str) -> None:
        self.execute(f'USE {_fo(name)}')

    def create_user(self, name:str, password:str, grants:List[str]) -> None:
        self.execute(f'CREATE USER {_fo(name)} IDENTIFIED BY "{password}"')
        self.execute('GRANT ' + ', '.join(grants) + f' ON * TO {_fo(name)}')

    def create_table(self, name:str, colname_types:Dict[str, str]) -> None:
        self.execute(f'CREATE TABLE {_fo(name)}(' + ', '.join(f'{_fo(cn)} {ct}' for cn, ct in colname_types.items()) + ')')

    def recreate_database(self, name:str, *args, **kwargs) -> None:
        self.drop_database(name)
        self.create_database(name, *args, **kwargs)

    def recreate_user(self, name:str, password:str, grants:List[str], *args, **kwargs) -> None:
        self.drop_user(name)
        self.create_user(name, password, grants, *args, **kwargs)

    def recreate_table(self, name:str, colname_types:Dict[str, str], *args, **kwargs) -> None:
        self.drop_table(name)
        self.create_table(name, colname_types, *args, **kwargs)
    
    def insert(self, tablename:str, **kwargs:Any) -> int:
        """ Execute insert query """
        self.execute(
            f'INSERT INTO {_fo(tablename)}(' + ', '.join(map(_fo, kwargs)) + ')'
             + ' VALUES(' + ', '.join('%s' for _ in kwargs) + ')',
            list(kwargs.values())
        )
        # self.execute('INSERT INTO ' + tablename + ' VALUES(' + ', '.join(['%s'] * len(args)) + ')', args) # In case of using normal list
        return self.cursor.lastrowid

    
    def select_eq(self, tablename:str, colnames:Optional[List[str]]=None, **kwargs:Any):
        """ Execute select query """
        sql = 'SELECT ' + (', '.join(map(_fo, colnames)) if colnames else '*') + ' FROM ' + _fo(tablename)
        if kwargs:
            sql += ' WHERE ' + ' AND '.join(_fo(k) + ' = %s' for k in kwargs)
        self.execute(sql, list(kwargs.values()))
        return self.fetchall()


    def select_eq_one(self, tablename:str, colnames:Optional[List[str]]=None, **kwargs:Any):
        _result = self.select_eq(tablename, colnames, **kwargs)
        if not len(_result):
            return None
        if len(_result) != 1:
            raise RuntimeError('Multiple results.')
        return _result[0]


    def exists(self, tablename:str, **kwargs):
        res = self.select_eq_one(tablename, ['COUNT(*) AS n'], **kwargs)
        return res.n != 0

    def existsmany(self):
        raise NotImplementedError()


    def update(self, tablename:str, _id_colname:str='id', *, id:int, **kwargs:Any):
        """ Execute update query """
        self.execute(
            f'UPDATE {_fo(tablename)} SET ' + ', '.join(_fo(k) + ' = %s' for k in kwargs) + f' WHERE {_fo(_id_colname)} = %s',
             [*kwargs.values(), id]
        )


    def insertmany(self, tablename:str, rows:Iterable[list], _index_start:int=0, **kwargs:Callable) -> int:
        """ Execute insert query many time """
        self.executemany(
            f'INSERT INTO {_fo(tablename)}(' + ', '.join(map(_fo, kwargs)) + ')'
             + ' VALUES(' + ', '.join(['%s'] * len(kwargs)) + ')',
            ([func(i, row) for func in kwargs.values()] for i, row in enumerate(rows, _index_start)),
        )
        return self.cursor.lastrowid


    def execproc(self, procname:str, args:list) -> int:
        """ Execute procedure or function defined in the Database """
        self.execute(f'SELECT {procname}(' + ', '.join(['%s'] * len(args)) + ') as `id`', args)
        return self.fetchjustone().id


    def query(self, sql:str, params:Optional[list]=None) -> Any:
        self.execute(sql, params)
        return self.fetchall()


    def query_one(self, sql:str, params:Optional[list]=None) -> Any:
        self.execute(sql, params)
        return self.fetchone()


    def execute(self, sql:str, params:Optional[list]=None):
        """ Execute SQL query """
        return self.cursor.execute(sql, [self.filter_param(p) for p in params] if params else None)


    def executemany(self, sql:str, params_list:Optional[List[list]]=None):
        """ Execute SQL query many time """
        return self.cursor.executemany(sql, [[self.filter_param(p) for p in params] for params in params_list] if params_list else None)


    def execute_from(self, f:IO):
        return self.execute(f.read())


    def filter_param(self, param):
        # if isinstance(param, str):
        #     return param.replace('\n', '\\n')
        return param


    def fetchjustone(self) -> Any:
        if not self.cursor:
            raise RuntimeError('Cursor not opened.')
        result = self.cursor.fetchall()
        if len(result) != 1:
            raise RuntimeError('Not just one result.')
        return result[0]


    def fetchone(self) -> Any:
        if not self.cursor:
            raise RuntimeError('Cursor not opened.')
        result = self.cursor.fetchall()
        if len(result) > 1:
            raise RuntimeError('Multiple result.')
        if result:
            return result[0]
        return None


    def fetchall(self):
        return self.cursor.fetchall()


    def close(self):
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None


    def __exit__(self, ex_type, ex_value, trace):
        self.close()


    def __del__(self):
        self.close()
