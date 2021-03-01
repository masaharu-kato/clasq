"""
    SQL Execution classes and functions
"""
from typing import Any, Callable, Dict, IO, Iterable, List, Optional, Union
from . import fmt
# from .query import QueryMaker
# from .cursor import MySQLCursor

_DUMP_EXECUTES = False

# SQLLike = Union[QueryMaker, str]
SQLLike = str


class TableInserts:
    def __init__(self, sqexec:'SQLExecutor', tablename:str, colnames:Optional[List[str]] = None):
        self.sqexec = sqexec
        self.tablename = tablename
        self.colnames = colnames
        self.records = []

    def reset(self):
        self.colnames = None
        self.records = []

    def __enter__(self):
        self.reset()
        return self

    def insert(self, *args, **kwargs):
        """ Prepare a insertion of new record """
        if self.colnames is None:
            if args:
                raise RuntimeError('Positional arguments cannot be used without specifying column names in initialization.')
            self.colnames = list(kwargs.keys())

        if args and kwargs:
            raise RuntimeError('Positional arguments and keyword arguments cannot be used at the same time.')

        if args:
            if len(args) != len(self.colnames):
                raise RuntimeError('The number of values does not match.')
            values = args
        elif kwargs:
            values = []
            for colname in self.colnames:
                if colname not in kwargs:
                    raise RuntimeError('Key `{}` is not specified.'.format(colname))
                self.records.append(kwargs[colname])

        self.records.append(args)

    def execute(self) -> int:
        """ Execute prepared insertions """
        self.sqexec.executemany(
            f'INSERT INTO {fmt.fo(self.tablename)}(' + ', '.join(map(fmt.fo, self.colnames)) + ')'
             + ' VALUES(' + ', '.join(['%s'] * len(self.colnames)) + ')',
             self.records
        )
        self.reset()
        return self.sqexec.cursor.lastrowid

    def __exit__(self, exc_type, exc_value, traceback):
        self.execute()


class SQLExecutor:
    """ SQL実行クラス """


    def __init__(self, sqcursor:'MySQLCursor'):
        """ Initialize a instance with a cursor to the Database """

        # if not isinstance(sqcursor, MySQLCursor):
        #     raise TypeError('Invalid type of cursor.')

        if sqcursor is None:
            raise RuntimeError('Cursor is None.')
        
        self.cursor = sqcursor.cursor
        # self.qmaker = QueryMaker(sqcursor.con.db_schema) if sqcursor.con.db_schema else None

    def __enter__(self):
        return self

    def drop_database(self, name:str) -> None:
        """ Drop database """
        self.execute(f'DROP DATABASE IF EXISTS {fmt.fo(name)}')

    def drop_user(self, name:str) -> None:
        """ Drop user """
        self.execute(f'DROP USER IF EXISTS {fmt.fo(name)}')

    def drop_table(self, name:str) -> None:
        """ Drop table """
        self.execute(f'DROP TABLE IF EXISTS {fmt.fo(name)}')

    def create_database(self, name:str) -> None:
        """ Create database """
        self.execute(f'CREATE DATABASE {fmt.fo(name)}')

    def use_database(self, name:str) -> None:
        """ Use database """
        self.execute(f'USE {fmt.fo(name)}')

    def create_user(self, name:str, password:str, grants:List[str]) -> None:
        """ Create user """
        self.execute(f'CREATE USER {fmt.fo(name)} IDENTIFIED BY "{password}"')
        self.execute('GRANT ' + ', '.join(grants) + f' ON * TO {fmt.fo(name)}')

    def create_table(self, name:str, colname_types:Dict[str, str]) -> None:
        """ Create table """
        self.execute(f'CREATE TABLE {fmt.fo(name)}(' + ', '.join(f'{fmt.fo(cn)} {fmt.sqltype(ct)}' for cn, ct in colname_types.items()) + ')')

    def create_function(self, name:str, arg_types:Dict[str, str], return_type:str, statements:Iterable[str], *, determinstic:bool=False) -> None:
        # TODO: Implementation
        """ Create function """
        raise NotImplementedError()

    def create_selsert_function(self, tablename:str, colname_types:Dict[str, str]) -> None:
        # TODO: Implementation
        self.execute(f'''
            DELIMITER //
            CREATE FUNCTION `selsert_{tablename}`
            (_itemname VARCHAR(64), _itemname_nonl VARCHAR(64)) RETURNS INT DETERMINISTIC
            BEGIN
                IF _itemname IS NULL THEN RETURN NULL; END IF;
                SET @ret_id = (SELECT `id` FROM `itemnames` WHERE `itemname` = _itemname);
                IF @ret_id IS NOT NULL THEN RETURN @ret_id; END IF;
                INSERT INTO `itemnames`(`itemname`, `itemname_nonl`) VALUES(_itemname, _itemname_nonl);
                RETURN LAST_INSERT_ID();
            END//
        ''')

    def recreate_database(self, name:str, *args, **kwargs) -> None:
        """ Recreate (drop and create) database """
        self.drop_database(name)
        self.create_database(name, *args, **kwargs)

    def recreate_user(self, name:str, password:str, grants:List[str], *args, **kwargs) -> None:
        """ Recreate (drop and create) user """
        self.drop_user(name)
        self.create_user(name, password, grants, *args, **kwargs)

    def recreate_table(self, name:str, colname_types:Dict[str, str], *args, **kwargs) -> None:
        """ Recreate (drop and create) table """
        self.drop_table(name)
        self.create_table(name, colname_types, *args, **kwargs)
    
    def insert(self, tablename:str, **kwargs:Any) -> int:
        """ Execute insert query """
        self.execute(
            f'INSERT INTO {fmt.fo(tablename)}(' + ', '.join(map(fmt.fo, kwargs)) + ')'
             + ' VALUES(' + ', '.join('%s' for _ in kwargs) + ')',
            list(kwargs.values())
        )
        # self.execute('INSERT INTO ' + tablename + ' VALUES(' + ', '.join(['%s'] * len(args)) + ')', args) # In case of using normal list
        return self.cursor.lastrowid

    def insert_to_table(self, tablename:str, colnames:Optional[List[str]]=None):
        return TableInserts(self, tablename, colnames)

    # def _select_query(self, *args, **kwargs):
    #     if self.qmaker is None:
    #         raise RuntimeError('Query maker is not initialized because schema is not specified.')
    #     return self.qmaker.select(*args, **kwargs)

    # def select(self, *args, **kwargs):
    #     """ Simple select: Execute select query """
    #     return self.query(*self._select_query(*args, **kwargs))

    # def uselect(self, *args, **kwargs):
    #     """ Unique select: Execute select query, assuming one result"""
    #     return self.query_one(*self._select_query(*args, **kwargs))

    # def pselect(self, *args, **kwargs):
    #     """ Select with parent(s): Execute select query with parent table(s) """
    #     return self.select(*args, parent_tables=True, **kwargs)

    # def puselect(self, *args, **kwargs):
    #     """ Unique select with parent(s): Execute select query with parent table(s), assuming one result """
    #     return self.uselect(*args, parent_tables=True, **kwargs)

    # def cselect(self, *args, **kwargs):
    #     """ Select with child(ren): Execute select query with child table(s) """
    #     return self.select(*args, child_tables=True, **kwargs)

    # def pcselect(self, *args, **kwargs):
    #     """ Select with parent(s) and child(ren): Execute select query with parent table(s) and child table(s) """
    #     return self.select(*args, parent_tables=True, child_tables=True, **kwargs)
    
    # def select_eq(self, tablename:str, colnames:Optional[List[str]]=None, **kwargs:Any):
    #     """ Execute select query for a single table with equivalence conditions """
    #     sql = 'SELECT ' + (', '.join(map(fmt.fo, colnames)) if colnames else '*') + ' FROM ' + fmt.fo(tablename)
    #     if kwargs:
    #         sql += ' WHERE ' + ' AND '.join(fmt.fo(k) + ' = %s' for k in kwargs)
    #     self.execute(sql, list(kwargs.values()))
    #     return self.fetchall()


    # def select_eq_one(self, tablename:str, colnames:Optional[List[str]]=None, **kwargs:Any):
    #     """ Execute select query for a single table with equivalence conditions, assuming one result """
    #     _result = self.select_eq(tablename, colnames, **kwargs)
    #     if not _result:
    #         return None
    #     if len(_result) != 1:
    #         raise RuntimeError('Multiple results.')
    #     return _result[0]


    # def exists(self, tablename:str, **kwargs):
    #     """ Check if exists specific record in the table """
    #     res = self.select_eq_one(tablename, ['COUNT(*) AS n'], **kwargs)
    #     return res.n != 0


    def update(self, tablename:str, _id_colname:str='id', *, id:int, **kwargs:Any): # pylint: disable=redefined-builtin
        """ Execute update query """
        self.execute(
            f'UPDATE {fmt.fo(tablename)} SET ' + ', '.join(fmt.fo(k) + ' = %s' for k in kwargs) + f' WHERE {fmt.fo(_id_colname)} = %s',
             [*kwargs.values(), id]
        )


    def insertmany(self, tablename:str, rows:Iterable[list], _index_start:int=0, **kwargs:Callable) -> int:
        """ Execute insert query many time """
        self.executemany(
            f'INSERT INTO {fmt.fo(tablename)}(' + ', '.join(map(fmt.fo, kwargs)) + ')'
             + ' VALUES(' + ', '.join(['%s'] * len(kwargs)) + ')',
            ([func(i, row) for func in kwargs.values()] for i, row in enumerate(rows, _index_start)),
        )
        return self.cursor.lastrowid


    def execproc(self, procname:str, args:list) -> int:
        """ Execute procedure or function defined in the Database """
        self.execute(f'SELECT {procname}(' + ', '.join(['%s'] * len(args)) + ') as `id`', args)
        return self.fetchjustone().id


    def query(self, sql:SQLLike, params:Optional[list]=None) -> Any:
        """ Run sql with parameters """
        self.execute(sql, params)
        return self.fetchall()


    def query_one(self, sql:SQLLike, params:Optional[list]=None) -> Any:
        """ Run sql with parameters, assume one result """
        self.execute(sql, params)
        return self.fetchone()


    def execute(self, sql:SQLLike, params:Optional[list]=None):
        """ Execute SQL query """
        if _DUMP_EXECUTES:
            print('Executor.execute():', sql, params)
        return self.cursor.execute(self._sql_str(sql), [self.filter_param(p) for p in params] if params else None)


    def executemany(self, sql:SQLLike, params_list:Optional[List[list]]=None):
        """ Execute SQL query many time """
        return self.cursor.executemany(self._sql_str(sql), [[self.filter_param(p) for p in params] for params in params_list] if params_list else None)


    @staticmethod
    def _sql_str(sql:SQLLike) -> str:
        """ Convert to SQL string """
        if isinstance(sql, str):
            return sql
        return sql.__sql__()


    def execute_from(self, f:IO):
        """ Execute SQL statements from file """
        return self.execute(f.read())


    def filter_param(self, param):
        """ filter the parameter value for SQL execution """
        # if isinstance(param, str):
        #     return param.replace('\n', '\\n')
        return param


    def fetchjustone(self) -> Any:
        """ Fetch just one record in result (if not, raise exception) """
        if not self.cursor:
            raise RuntimeError('Cursor not opened.')
        result = self.cursor.fetchall()
        if len(result) != 1:
            raise RuntimeError('Not just one result.')
        return result[0]


    def fetchone(self) -> Any:
        """ Fetch only one record in result (or no result as None) (if multiple, raise exception) """
        if not self.cursor:
            raise RuntimeError('Cursor not opened.')
        result = self.cursor.fetchall()
        if len(result) > 1:
            raise RuntimeError('Multiple result.')
        if result:
            return result[0]
        return None


    def fetchall(self):
        """ Fetch all records in result """
        return self.cursor.fetchall()


    def close(self):
        """ Close cursur if not closed """
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None


    def __exit__(self, ex_type, ex_value, trace):
        self.close()


    def __del__(self):
        self.close()
