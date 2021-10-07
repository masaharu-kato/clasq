"""
    SQL Execution classes and functions
"""
from typing import Any, Callable, Dict, IO, Iterable, Iterator, List, Optional, Sequence, Set, Tuple, TypeVar, Union
import warnings
from libsql.connector import CursorABC
import mysql.connector.errors as mysql_error # type: ignore
from .schema import Column, ColumnAlias, ColumnLike, Database, ExtraColumnExpr, JoinLike, JoinType, OrderLike, OrderType, Table, TableLike, TableLink, asobj, astext, astype

T = TypeVar('T')
SQLLike = str

class BasicQueryExecutor:
    """ SQL実行クラス """

    def __init__(self, cursor:CursorABC):
        """ Initialize a instance with a cursor to the Database """
        assert isinstance(cursor, CursorABC)
        self._cursor:Optional[CursorABC] = cursor

    @property
    def db(self) -> Database:
        return self.cursor.db

    @property
    def cursor(self) -> CursorABC:
        if self._cursor is None:
            raise RuntimeError('Cursor is already closed.')
        return self._cursor

    def __enter__(self):
        return self

    def execproc(self, procname:str, args:list) -> int:
        """ Execute procedure or function defined in the Database """
        try:
            self.execute(f'SELECT {procname}(' + ', '.join(['%s'] * len(args)) + ') as `id`', args)
            return self.fetchjustone().id
        except mysql_error.DataError as e:
            raise RuntimeError('Errors in arguments on the function `{}` (args={})'.format(procname, repr(args))) from e

    def query(self, sql:SQLLike, params:Optional[list]=None, *, fetch_one:bool=False) -> list:
        """ Run sql with parameters """
        try:
            self.execute(sql, params)
            return self.fetchall() if not fetch_one else self.fetchone()
        except Exception as e:
            raise RuntimeError('Failed to query SQL: %s, Args: %s' % (sql, ', '.join(map(str, params)) if params is not None else 'None')) from e

    def query_one(self, sql:SQLLike, params:Optional[list]=None) -> Any:
        return self.query(sql, params, fetch_one=True)

    def execute(self, sql:SQLLike, params:Optional[list]=None):
        """ Execute SQL query """
        return self.cursor.execute(sql, [self.filter_param(p) for p in params] if params else None)

    def executemany(self, sql:SQLLike, params_list:Iterable[Union[List, Tuple]]):
        """ Execute SQL query many time """
        return self.cursor.executemany(sql, [[self.filter_param(p) for p in params] for params in params_list])

    def execute_from(self, f:IO):
        """ Execute SQL statements from file """
        return self.execute(f.read())

    def filter_param(self, param):
        """ filter the parameter value for SQL execution """
        return param

    def fetchjustone(self) -> Any:
        """ Fetch just one record in result (if not, raise exception) """
        result = self.fetchall()
        if len(result) != 1:
            raise RuntimeError('Not just one result.')
        return result[0]

    def fetchone(self) -> Any:
        """ Fetch only one record in result (or no result as None) (if multiple, raise exception) """
        result = self.fetchall()
        if len(result) > 1:
            raise RuntimeError('Multiple result.')
        if result:
            return result[0]
        return None

    def fetchall(self):
        """ Fetch all records in result """
        return self.cursor.fetchall()

    def closed(self) -> bool:
        return self._cursor is None

    def close(self):
        """ Close cursur if not closed """
        if not self.closed():
            self.cursor.close()
            self._cursor = None

    def __exit__(self, ex_type, ex_value, trace):
        self.close()

    def __del__(self):
        self.close()


class QueryExecutor(BasicQueryExecutor):

    def drop_database(self, name:str) -> None:
        """ Drop database """
        self.execute(f'DROP DATABASE IF EXISTS {asobj(name)}')

    def drop_user(self, name:str) -> None:
        """ Drop user """
        self.execute(f'DROP USER IF EXISTS {asobj(name)}')

    def drop_table(self, name:str) -> None:
        """ Drop table """
        self.execute(f'DROP TABLE IF EXISTS {asobj(name)}')

    def create_database(self, name:str) -> None:
        """ Create database """
        self.execute(f'CREATE DATABASE {asobj(name)}')

    def use_database(self, name:str) -> None:
        """ Use database """
        self.execute(f'USE {asobj(name)}')

    def create_user(self, name:str, password:str, grants:List[str]) -> None:
        """ Create user """
        self.execute(f'CREATE USER {asobj(name)} IDENTIFIED BY "{password}"')
        self.execute('GRANT ' + ', '.join(grants) + f' ON * TO {asobj(name)}')

    def create_table(self, name:str, colname_types:Dict[str, str]) -> None:
        """ Create table """
        self.execute(f'CREATE TABLE {asobj(name)}(' + ', '.join(f'{asobj(cn)} {astype(ct)}' for cn, ct in colname_types.items()) + ')')

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

    def recreate_database(self, name:str) -> None:
        """ Recreate (drop and create) database """
        self.drop_database(name)
        self.create_database(name)

    def recreate_user(self, name:str, password:str, grants:List[str]) -> None:
        """ Recreate (drop and create) user """
        self.drop_user(name)
        self.create_user(name, password, grants)

    def recreate_table(self, name:str, colname_types:Dict[str, str]) -> None:
        """ Recreate (drop and create) table """
        self.drop_table(name)
        self.create_table(name, colname_types)
    
    def insert(self, tablename:str, **kwargs:Any) -> int:
        """ Execute insert query """
        self.execute(
            f'INSERT INTO {asobj(tablename)}(' + ', '.join(map(asobj, kwargs)) + ')'
             + ' VALUES(' + ', '.join('%s' for _ in kwargs) + ')',
            list(kwargs.values())
        )
        # self.execute('INSERT INTO ' + tablename + ' VALUES(' + ', '.join(['%s'] * len(args)) + ')', args) # In case of using normal list
        return self.cursor.last_row_id()

    def insert_to_table(self, tablename:str, colnames:Optional[List[str]]=None):
        return TableInserts(self, tablename, colnames)

    def exists(self, tablename:str, **kwargs):
        """ Check if exists specific record in the table """
        res = self.select_eq_one(tablename, ['COUNT(*) AS n'], **kwargs)
        return res.n != 0


    def update(self, tablename:str, _id_colname:str='id', *, id:int, **kwargs:Any): # pylint: disable=redefined-builtin
        """ Execute update query """
        self.execute(
            f'UPDATE {asobj(tablename)} SET ' + ', '.join(asobj(k) + ' = %s' for k in kwargs) + f' WHERE {asobj(_id_colname)} = %s',
             [*kwargs.values(), id]
        )

    def insertmany(self, tablename:str, rows:Iterable[list], _index_start:int=0, **kwargs:Callable) -> int:
        """ Execute insert query many time """
        self.executemany(
            f'INSERT INTO {asobj(tablename)}(' + ', '.join(map(asobj, kwargs)) + ')'
             + ' VALUES(' + ', '.join(['%s'] * len(kwargs)) + ')',
            ([func(i, row) for func in kwargs.values()] for i, row in enumerate(rows, _index_start)),
        )
        return self.cursor.last_row_id()

#   ================================================================================================================================
#       SELECT query methods
#   ================================================================================================================================

    def select(self, *args, **kwargs):
        return self.query(*self._select_query(*args, **kwargs))

    def select_one(self, *args, **kwargs):
        """ Unique select: Execute select query, assuming one result"""
        return self.query_one(*self._select_query(*args, **kwargs))
    
    def select_eq(self, tablename:str, colnames:Optional[List[str]]=None, **kwargs:Any):
        """ Execute select query for a single table with equivalence conditions """
        sql = 'SELECT ' + (', '.join(map(asobj, colnames)) if colnames else '*') + ' FROM ' + asobj(tablename)
        if kwargs:
            sql += ' WHERE ' + ' AND '.join(asobj(k) + ' = %s' for k in kwargs)
        self.execute(sql, list(kwargs.values()))
        return self.fetchall()

    def select_eq_one(self, tablename:str, colnames:Optional[List[str]]=None, **kwargs:Any):
        """ Execute select query for a single table with equivalence conditions, assuming one result """
        _result = self.select_eq(tablename, colnames, **kwargs)
        if not _result:
            return None
        if len(_result) != 1:
            raise RuntimeError('Multiple results.')
        return _result[0]

    def _select_query(self, *,
        table          : Optional[TableLike]                          = None, # Base table
        join_tables    : Optional[List[Tuple[TableLike, JoinLike]]]   = None, # Tables to join
        tables         : Optional[List[TableLike]]                    = None, # Tables to join
        opt_table      : Optional[TableLike]                          = None, # Optional table to join
        opt_tables     : Optional[List[TableLike]]                    = None, # Optional tables to join
        extra_columns  : Optional[List[Tuple[str, str]]]              = None, # Extra select column expression and its aliases
        where_sql      : Optional[str]                                = None, # Where SQL
        where_params   : Optional[list]                               = None, # Where SQL parameters
        where_eq       : Optional[Tuple[ColumnLike, Any]]             = None, # Where equal formula
        where_eqs      : Optional[List[Tuple[ColumnLike, Any]]]       = None, # Where equal formulas
        group          : Optional[ColumnLike]                         = None, # Grouping column
        groups         : Optional[List[ColumnLike]]                   = None, # Grouping columns
        order          : Optional[Tuple[ColumnLike, OrderLike]]       = None, # Ordering column and its kind (ASC or DESC)
        orders         : Optional[List[Tuple[ColumnLike, OrderLike]]] = None, # Ordering columns and its kinds
        limit          : Optional[int]                                = None, # Limit of results
        offset         : Optional[int]                                = None, # Offset of results
        skip_same_alias: bool = True,
    ) -> Tuple[str, list]:
        """
            Calculate SQL SELECT query
            returns (SQL statement string, parameter values)    
        """
        return self._select_query_by_list(
            tables     = list(self._one_or_more(table, tables)),
            opt_tables = list(self._one_or_more(opt_table, opt_tables)),
            where_eqs  = list(self._one_or_more(where_eq, where_eqs)),
            groups     = list(self._one_or_more(group, groups)),
            orders     = list(self._one_or_more(order, orders)),
            join_tables= join_tables or [], extra_columns=extra_columns or [], where_sql=where_sql, where_params=where_params, limit=limit, offset=offset,
            skip_same_alias=skip_same_alias
        )

    def _select_query_by_list(self, *,
        tables         : List[TableLike]                   , # Tables
        opt_tables     : List[TableLike]                   , # Optional tables to join
        join_tables    : List[Tuple[TableLike, JoinLike]]  , # Tables to join
        extra_columns  : List[Tuple[str, str]]             , # Extra select column expression and its aliases
        where_eqs      : List[Tuple[ColumnLike, Any]]      , # Where equal formulas
        groups         : List[ColumnLike]                  , # Grouping columns
        orders         : List[Tuple[ColumnLike, OrderLike]], # Ordering columns and its kinds
        where_sql      : Optional[str]               = None, # Where SQL
        where_params   : Optional[list]              = None, # Where SQL parameters
        limit          : Optional[int]               = None, # Limit of results
        offset         : Optional[int]               = None, # Offset of results
        skip_same_alias: bool = True,
    ) -> Tuple[str, list]:

        if not tables:
            raise RuntimeError('No tables specified.')

        # Process join tables 
        _join_tables = [(self.db.table(tablelike), JoinType(jointype)) for tablelike, jointype in join_tables]
        _join_tables.extend((self.db.table(tablelike), JoinType.INNER) for tablelike in tables[1:])
        _join_tables.extend((self.db.table(tablelike), JoinType.LEFT ) for tablelike in opt_tables)

        # Process where clause
        _where_sqls = [where_sql] if where_sql else []
        _where_params = [] if where_params is None else [*where_params]

        _where_eqs = [(self.db.column(columnlike), value) for columnlike, value in where_eqs]
        for column, value in _where_eqs:
            if value is None:
                _where_sqls.append(f'{column.sql()} IS NULL')
            else:
                _where_sqls.append(f'{column.sql()} = %s')
                _where_params.append(value)
        
        return self._construct_select_query(
            base_table    = self.db.table(tables[0]),
            join_tables   = _join_tables,
            extra_columns = [(ExtraColumnExpr(cexpr), ColumnAlias(alias)) for cexpr, alias in extra_columns],
            where_sql     = ' AND '.join(_where_sqls),
            where_params  = _where_params,
            groups        = [self.db.column(col) for col in groups],
            orders        = [(self.db.column(col), OrderType(odt)) for col, odt in orders],
            limit         = limit,
            offset        = offset,
            skip_same_alias = skip_same_alias,
        )


    def _construct_select_query(self, *,
        base_table      : Table,
        join_tables     : List[Tuple[Table, JoinType]],
        extra_columns   : List[Tuple[ExtraColumnExpr, ColumnAlias]],
        where_sql       : Optional[str],
        where_params    : list,
        groups          : List[Column],
        orders          : List[Tuple[Column, OrderType]],
        limit           : Optional[int],
        offset          : Optional[int],
        skip_same_alias : bool,
    ) -> Tuple[str, list]:

        # Prepare columns to select
        select_columns:List[Tuple[Column, Optional[ColumnAlias]]] = []
        used_column_names:Set[ColumnAlias] = set()

        for column in self.db[base_table].columns:
            select_columns.append((column, None))
            used_column_names.add(column.name)

        for table, _ in join_tables:
            for column in self.db.table(table).columns:
                if not column.name in used_column_names:
                    select_columns.append((column, None))
                    used_column_names.add(column.name)
                else:
                    _alias = self.AUTO_ALIAS_FUNC(column)
                    if _alias in used_column_names:
                        if not skip_same_alias:
                            raise RuntimeError('Alias `{}` already used.'.format(_alias))
                    else:
                        select_columns.append((column, _alias))
                        used_column_names.add(_alias)

        # Construct SELECT column expressions
        select_extra_columns:List[Tuple[ExtraColumnExpr, ColumnAlias]] = []
        for column_expr, alias in extra_columns:
            if alias in used_column_names:
                raise RuntimeError('Alias `{}` already used.'.format(alias))
            select_extra_columns.append((column_expr, alias))
            used_column_names.add(alias)

        select_col_exprs = []
        for column, opt_alias in select_columns:
            select_col_exprs.append(column.sql() + ((' AS ' + astext(opt_alias)) if opt_alias is not None else ''))
        for column_expr, alias in select_extra_columns:
            select_col_exprs.append(column_expr + ' AS ' + astext(alias))

        # Construct Joins
        joins:List[Tuple[JoinType, TableLink]] = []
        _loaded_tables = [base_table]
        for target_table, jointype in join_tables:
            clink:Sequence[TableLink] = list(target_table.find_tables_links(_loaded_tables))
            if not len(clink):
                raise RuntimeError('No links found between table `{}` from tables {}.'.format(target_table, ', '.join('`{}`'.format(t) for t in _loaded_tables)))
            if len(clink) > 1:
                warnings.warn(RuntimeWarning('Multiple links found between table `{}` from tables {}.'.format(target_table, ', '.join('`{}`'.format(t) for t in _loaded_tables))))
                # raise RuntimeError('Multiple links found between table `{}` from tables {}.'.format(target_table, ', '.join('`{}`'.format(t) for t in _loaded_tables)))
            joins.append((jointype, clink[0]))
            _loaded_tables.insert(0, target_table)

        # Construct SQL statement
        sql = 'SELECT ' + ', '.join(select_col_exprs) + ' FROM ' + base_table.sql() + '\n'

        for jointype, tlink in joins:
            sql += f' {self._fmt_jointype(jointype)} JOIN {tlink.lcol.table.sql()} ON {tlink.lcol.sql()} = {tlink.rcol.sql()}\n'

        params:List[Any] = []
        if where_sql:
            sql += ' WHERE ' + where_sql + '\n'
            params.extend(where_params)

        if groups:
            sql += ' GROUP BY ' + ', '.join(g.sql() for g in groups) + '\n'

        if orders:
            sql += ' ORDER BY ' + ', '.join(f'{column.sql()} {self._fmt_ordertype(ordertype)}' for column, ordertype in orders) + '\n'

        if limit is not None:
            sql += ' LIMIT %s\n'
            params.append(limit)

        if offset is not None:
            sql += ' OFFSET %s\n'
            params.append(offset)

        # Return a tuple of (SQL statement, list of parameter values)
        return sql, params

    @staticmethod
    def _one_or_more(one:Optional[T], more:Optional[Iterable[T]]) -> Iterator[T]:
        if one is not None:
            yield one
        if more is not None:
            yield from more

    @staticmethod
    def _fmt_jointype(jointype:JoinType):
        if not isinstance(jointype, JoinType):
            raise RuntimeError('Invalid join type %s' % jointype)
        return jointype.name

    @staticmethod
    def _fmt_ordertype(ordertype:OrderType):
        if not isinstance(ordertype, OrderType):
            raise RuntimeError('Invalid ordertype %s' % ordertype)
        return ordertype.name

    @staticmethod
    def AUTO_ALIAS_FUNC(column:Column) -> ColumnAlias:
        """ Automatically aliasing function for columns expressions in SELECT query """
        return ColumnAlias(column.table.name + '_' + column.name)



class TableInserts:
    """ Table insertion utility class """

    def __init__(self, sqexec:'QueryExecutor', tablename:str, colnames:Optional[List[str]] = None):
        self.sqexec = sqexec
        self.tablename = tablename
        self.colnames = colnames
        self.records:List[List] = []

    def reset(self):
        """ Reset data """
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
                values.append(kwargs[colname])
        else:
            raise RuntimeError('No arguments are specified.')

        assert(isinstance(values, list))
        self.records.append(values)

    def execute(self) -> Optional[int]:
        """ Execute prepared insertions """
        if not (self.colnames and self.records):
            warnings.warn('No records inserted to table `{}`'.format(self.tablename))
            return None
        assert(isinstance(self.records, list) and all(isinstance(record, list) for record in self.records))

        self.sqexec.executemany(
            f'INSERT INTO {asobj(self.tablename)}(' + ', '.join(map(asobj, self.colnames)) + ')'
             + ' VALUES(' + ', '.join(['%s'] * len(self.colnames)) + ')',
             self.records
        )
        self.reset()
        return self.sqexec.cursor.last_row_id()

    def __exit__(self, exc_type, exc_value, traceback):
        self.execute()
