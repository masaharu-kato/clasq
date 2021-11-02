"""
    SQL Execution classes and functions
"""
from typing import Any, Callable, Dict, IO, Iterable, Iterator, List, Optional, Sequence, Set, Tuple, TypeVar, Union
import warnings
import sys

from libsql.connector import CursorABC
import mysql.connector.errors as mysql_error # type: ignore
from .schema import Column, ColumnAlias, ColumnLike, Database, ExtraColumnExpr, JoinLike, JoinType, OrderLike, OrderType, Table, TableLike, TableLink, asobj, asop, astext, astype

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
    
    def insert(self, tablelike: TableLike, **kwargs:Any) -> int:
        """ Execute insert query
        
            example:
                `self.insert('students', name="New name", age=26)`
                    ==> SQL: `INSERT students(name, age) VALUES(%s, %s)`, parameters: `["New name", 26]`
                
                * Returns a id value of the inserted record 
        """
        table = self.db.table(tablelike)
        self.execute(
            f'INSERT INTO {table.sql()}(' + ', '.join('`%s`' % table[colname].name for colname in kwargs) + ')'
                + ' VALUES(' + ', '.join('%s' for _ in kwargs) + ')',
            list(kwargs.values())
        )
        # self.execute('INSERT INTO ' + tablename + ' VALUES(' + ', '.join(['%s'] * len(args)) + ')', args) # In case of using normal list
        return self.cursor.last_row_id()

    def insertmany(self, tablelike: TableLike, rows: Iterable[list], _unpack=False, **kwargs: Any) -> int:
        """ Execute insert query many time
        
            ex1:
                ```
                    data = [('hoge name', 24), ('fugar nome', 22), ...]
                    self.insert('students', data,
                        name = lambda row: row[0],
                        age  = lambda row: row[1],
                    )
                ```
                    ==> SQL: `INSERT students(name, age) VALUES(%s, %s)`,
                        parameters for executemany: `[["hoge name", 26], ["fugar nome", 22], ...]`
                
                * Returns a id value of the first inserted record (if the MySQL/MariaDB environment)
                
        
            ex2:
                ```
                    plan_id = 76
                    data = [('action 1', 30), ('action 2', 15), ...]
                    self.insert('plan_actions', enumerate(data, 1), _unpack=True,
                        plan_id = plan_id,
                        index   = lambda i, row: i,
                        action  = lambda i, row: row[0],
                        time    = lambda i, row: row[1],
                    )
                ```
                    ==> SQL: `INSERT plan_actions(plan_id, index, action, time) VALUES(%s, %s, %s, %s)`,
                        parameters for executemany: `[[76, 1, "action 1", 30], [76, 2, "action 2", 15], ...]`
        """
        table = self.db.table(tablelike)

        # row_maker = {}
        # for colname, v in kwargs.items():
        #     print('colname:', colname)
        #     # breakpoint()
        #     # v = (lambda i, row: 'i=%d, row=%s' % (i, repr(row)))
        #     row_maker[colname] = lambda _row, v=v: (v(*_row))

        row_maker = {
            colname: ((lambda row, v=v: v(*row)) if _unpack else v) if callable(v) else lambda _, v=v: v  # capture the current `v` in `v=v
            for colname, v in kwargs.items()
        }
        self.executemany(
            f'INSERT INTO {table.sql()}(' + ', '.join('`%s`' % table[colname].name for colname in kwargs) + ')'
                + ' VALUES(' + ', '.join(['%s'] * len(kwargs)) + ')',
            ([row_maker[colname](row) for colname in kwargs] for row in rows),
        )
        return self.cursor.last_row_id()

    def insert_to_table(self, tablename:str, colnames:Optional[List[str]]=None):
        return TableInserts(self, tablename, colnames)

    def exists(self, tablelike: TableLike, **options):
        """ Check if exists specific record in the table """
        res = self.select(tablelike, extra_columns=[('COUNT(*)', 'n')], **options)
        return res.n != 0

    def update(self, tablelike: TableLike, *, id: int, **kwargs: Any) -> None: # pylint: disable=redefined-builtin
        """ Execute update query
            
            example:
                `self.update('students', id=105, name="New name", age=26)`
                    ==> SQL: `UPDATE students SET name = %s AND age = %s WHERE id = %s`, parameters: `["New name", 26, 105]`
        """
        table = self.db.table(tablelike)
        self.execute(
            f'UPDATE {table.sql()} SET ' + ', '.join(('`%s`' % table[colname].name) + ' = %s' for colname in kwargs) + f' WHERE {table.keycol.name} = %s',
             [*kwargs.values(), id]
        )

#   ================================================================================================================================
#       SELECT query method
#   ================================================================================================================================

    def select(self,
        *_tables       : TableLike,                                           # Tables to join
        table          : Optional[TableLike]                          = None, # Base table
        tables         : Optional[List[TableLike]]                    = None, # Tables to join
        join_tables    : Optional[List[Tuple[TableLike, JoinLike]]]   = None, # Tables to join
        join_ons       : Optional[Dict[TableLike, Tuple[str, list]]]  = None, # Terms in join `ON` clause
        opt_table      : Optional[TableLike]                          = None, # Optional table to join
        opt_tables     : Optional[List[TableLike]]                    = None, # Optional tables to join
        extra_columns  : Optional[List[Tuple[str, str]]]              = None, # Extra select column expression and its aliases
        where_sql      : Optional[str]                                = None, # Where SQL
        where_params   : Optional[list]                               = None, # Where SQL parameters
        where_op       : Optional[Tuple[ColumnLike, Any]]             = None, # Where binary operator formula
        where_ops      : Optional[List[Tuple[ColumnLike, Any]]]       = None, # Where binary operator formulas
        where_eq       : Optional[Tuple[ColumnLike, Any]]             = None, # Where equal formula
        where_eqs      : Optional[List[Tuple[ColumnLike, Any]]]       = None, # Where equal formulas
        where_not_eq   : Optional[Tuple[ColumnLike, Any]]             = None, # Where not equal formula
        where_not_eqs  : Optional[List[Tuple[ColumnLike, Any]]]       = None, # Where not equal formulas
        id             : Optional[Any]                                = None, # Where id = value formula
        group          : Optional[ColumnLike]                         = None, # Grouping column
        groups         : Optional[List[ColumnLike]]                   = None, # Grouping columns
        order          : Optional[Tuple[ColumnLike, OrderLike]]       = None, # Ordering column and its kind (ASC or DESC)
        orders         : Optional[List[Tuple[ColumnLike, OrderLike]]] = None, # Ordering columns and its kinds
        limit          : Optional[int] = None, # Limit of results
        offset         : Optional[int] = None, # Offset of results
        one            : bool = False,         # Expect a just one result, and return it
        skip_same_alias: bool = True,          # Skip the error(s) of same alias name(s)
        dump           : bool = False,         # Dump a constructed SQL statement and parameters to the stderr
    ) -> Any:
        """
            SQL SELECT query
            returns: query result
                If `one` option is True, return a record
                If `one` option is False, return a list of records


            Table specification examples:

            ex1. (single table) 
                self.select('students', ...)           (recommended)  ┐     
                self.select(table='students', ...)                    │
                self.select(tables=('students',), ...)                ├─┐ These notations have the same effect 
                self.select(tables=['students'], ...)                 ┘ │
                    ==> SELECT * FROM `students` ...                 <──┘

                * `*` in the `SELECT *` will be replaced with the all columns specification, based on the schema.
                  If the `students` table has columns of `id`, `name`, `args`,
                  SELECT * FROM `students`  will be  SELECT `id`, `name`, `args` FROM `students` . 

            ex2. (multiple tables)
                self.select('students', 'classes', ...)                     (recommended)  
                self.select(tables=('students', 'classes', ...), ...)
                self.select(tables=['students', 'classes', ...], ...)
                self.select(table='students', tables=('classes', ...), ...)
                self.select(table='students', tables=['classes', ...], ...)
                    ==> SELECT * FROM `students` INNER JOIN `classes` ON ...

                * Table Joins are performed based on the schema.


            `id` argument example:

            ex. self.select('students', id=105)
                    ==> SQL: "SELECT * FROM `students` WHERE (`id` = %s)", parameters: [105]

                * `one` option will be automatically set to True if `id` argument is specified.
                  Therefore, returns a just one record. (If not exists, returns None.)

                * This specification is available only the column named `id`.
                  (For the other column(s), use the where_eq or where_eqs arguments.)


            Where arguments examples:

            ex1. (single equal term)
                self.select('students', where_eq=('name', 'hogename'))                     (recommended)
                self.select('students', where_eqs=[('name', 'hogename')])
                self.select('students', where_op=('name', '=', 'hogename'))
                self.select('students', where_ops=[('name', '=', 'hogename')])
                self.select('students', where_sql='name = %s', where_params=['hogename'])  (not recommended)
                    ==> SQL: "SELECT * FROM `students` WHERE (`name` = %s)", parameters: ['hogename']

            ex1. (single not equal term)
                self.select('students', where_not_eq=('name', 'hogename'))                     (recommended)
                self.select('students', where_not_eqs=[('name', 'hogename')])
                self.select('students', where_op=('name', '<>', 'hogename'))
                self.select('students', where_ops=[('name', '<>', 'hogename')])
                self.select('students', where_sql='name <> %s', where_params=['hogename'])  (not recommended)
                    ==> SQL: "SELECT * FROM `students` WHERE (`name` <> %s)", parameters: ['hogename']

            ex3. (multiple equal term)
                self.select('students', where_eqs=[('name', 'hogename'), ('age', 23)])                      (recommended)
                self.select('students', where_ops=[('name', '=', 'hogename'), ('age', '=', 23)])
                self.select('students', where_sql='name = %s AND age = %s', where_params=['hogename', 23])  (not recommended)
                    ==> SQL: "SELECT * FROM `students` WHERE (`name` = %s) AND (`age` = %s)", parameters: ['hogename', 23]

            ex4. (multiple less/more than terms)
                self.select('students', where_ops=[('age', '>=', 19), ('age', '<=', 22)])
                    ==> SQL: "SELECT * FROM `students` WHERE (age >= %s) AND (age <= %s)", parameters: [19, 22]

            ex5. (and/or terms) 
                                           ┌───────────────────────────────────────────────────────────────── AND ──────────────────────────────────────────────────────────────────┐
                                             ┌───────────────────────────── OR ───────────────────────────────┐  ┌───────────────────────────── OR ───────────────────────────────┐
                                                                   ┌───────────────── AND ──────────────────┐                          ┌───────────────── AND ──────────────────┐
                self.select(..., where_ops=[ [('year', '>', 2020), [('year', '=', 2020), ('month', '>=', 6))] ], [('year', '<', 2022), [('year', '=', 2022), ('month', '<=', 8))] ] ])

                ==> SQL: "SELECT ... WHERE ((year > %s) OR ((year = %s) AND (month >= %s))) AND ((year < %s) OR ((year = %s) AND (month <= %s)))", parameters: [2020, 2020, 6, 2022, 2022, 8]

                * The nested lists (Python list objects) will be joined in AND -> OR -> AND -> ... order.
                * This and/or specification is only valid on `where_op` or `where_ops` arguments.
                    (If the list is specified to `where_op` argument, its list will be joined in OR -> AND -> OR -> ... order.)

            ex6. (IS NULL term)
                self.select('students', where_eq=('age', None))           (recommended)
                self.select('students', where_ops=[('age', 'IS', None)])
                    ==> SQL: "SELECT * FROM `students` WHERE `age` IS NULL", parameters: []

                * The equal term with the `None` value (Python None) will be a `IS NULL` term.

            ex7. (IS NOT NULL term)
                self.select('students', where_not_eq=('age', None))       (recommended)
                self.select('students', where_ops=[('age', 'IS NOT', None)])
                    ==> SQL: "SELECT * FROM `students` WHERE `age` IS NOT NULL", parameters: []

                * The not equal term with the `None` value (Python None) will be a `IS NOT NULL` term.

        """
        return self.query(*self._select_query_by_list(
            tables        = list(self._one_or_more(table, _tables, tables)),
            opt_tables    = list(self._one_or_more(opt_table, opt_tables)),
            where_ops     = list(self._one_or_more(where_op, where_ops)),
            where_eqs     = list(self._one_or_more(('id', id) if id is not None else None, self._one_or_more(where_eq, where_eqs))),
            where_not_eqs = list(self._one_or_more(where_not_eq, where_not_eqs)),
            groups        = list(self._one_or_more(group, groups)),
            orders        = list(self._one_or_more(order, orders)),
            join_tables   = join_tables or [], extra_columns=extra_columns or [], where_sql=where_sql, where_params=where_params, limit=limit, offset=offset,
            join_ons      = {} if join_ons is None else join_ons,
            skip_same_alias=skip_same_alias,
            dump=dump
        ), fetch_one=(id is not None or one))

    def _select_query_by_list(self, *,
        tables         : List[TableLike]                   , # Tables
        opt_tables     : List[TableLike]                   , # Optional tables to join
        join_tables    : List[Tuple[TableLike, JoinLike]]  , # Tables to join
        join_ons       : Dict[TableLike, Tuple[str, list]] , # Terms on joion `ON` clause
        extra_columns  : List[Tuple[str, str]]             , # Extra select column expression and its aliases
        where_ops      : List[Union[Tuple[ColumnLike, str, Any], list]] , # Where binary operator formulas (list for OR operator)
        where_eqs      : List[Tuple[ColumnLike, Any]]      , # Where equal formulas
        where_not_eqs  : List[Tuple[ColumnLike, Any]]      , # Where not equal formulas
        groups         : List[ColumnLike]                  , # Grouping columns
        orders         : List[Tuple[ColumnLike, OrderLike]], # Ordering columns and its kinds
        where_sql      : Optional[str]               = None, # Where SQL
        where_params   : Optional[list]              = None, # Where SQL parameters
        limit          : Optional[int]               = None, # Limit of results
        offset         : Optional[int]               = None, # Offset of results
        skip_same_alias: bool = True,
        dump           : bool = False,
    ) -> Tuple[str, list]:
        """
            Construct SELECT SQL query
        """

        if not tables:
            raise RuntimeError('No tables specified.')

        # Process join tables 
        _join_tables = [(self.db.table(tablelike), JoinType(jointype)) for tablelike, jointype in join_tables]
        _join_tables.extend((self.db.table(tablelike), JoinType.INNER) for tablelike in tables[1:])
        _join_tables.extend((self.db.table(tablelike), JoinType.LEFT ) for tablelike in opt_tables)

        pr_tables = [*tables, *opt_tables, *(table for table, _ in join_tables)]

        # Process where equals
        try:
            where_ops.extend((columnlike, '=', value) for columnlike, value in where_eqs)
            where_ops.extend((columnlike, '<>', value) for columnlike, value in where_not_eqs)
        except ValueError as e:
            raise RuntimeError('Invalid eq or not_eq parameters: ', where_eqs, where_not_eqs) from e

        def _proc_where_ops(_where_ops:list, *, sqls:Optional[list]=None, prms:Optional[list]=None, is_and:bool):
            sqls = [] if sqls is None else sqls
            prms = [] if prms is None else prms
            for _where_op in _where_ops:
                if isinstance(_where_op, list):
                    _sql, _prms = _proc_where_ops(_where_op, is_and=not is_and)
                    sqls.append(_sql)
                    prms.extend(_prms)
                    continue
                
                try:
                    columnlike, _op, value = _where_op
                except ValueError as e:
                    raise RuntimeError('Invalid operator expression: %s' % _where_op) from e
                op = asop(_op)
                column = self.db.column(columnlike, pr_tables)
                if value is not None:
                    sqls.append(f'{column.sql()} {op} %s')
                    prms.append(value)
                else:
                    if op in ('=', 'IS'):
                        sqls.append(f'{column.sql()} IS NULL')
                    elif op in ('<>', '!=', 'IS NOT'):
                        sqls.append(f'{column.sql()} IS NOT NULL')
                    else:
                        raise RuntimeError('Invalid operator for NULL (None) value')
            return (' AND ' if is_and else ' OR ').join(['(%s)' % sql for sql in sqls]), prms

        where_sql, where_params = _proc_where_ops(
            where_ops,
            sqls = [where_sql] if where_sql else None,
            prms = [*where_params] if where_params is not None else None,
            is_and=True,
        )
        
        return self._construct_select_query(
            base_table    = self.db.table(tables[0]),
            join_tables   = _join_tables,
            join_ons      = {self.db.table(table): (expr, params) for table, (expr, params) in join_ons.items()},
            extra_columns = [(ExtraColumnExpr(cexpr), ColumnAlias(alias)) for cexpr, alias in extra_columns],
            where_sql     = where_sql,
            where_params  = where_params,
            groups        = [self.db.column(col, pr_tables) for col in groups],
            orders        = [(self.db.column(col, pr_tables), OrderType(odt)) for col, odt in orders],
            limit         = limit,
            offset        = offset,
            skip_same_alias = skip_same_alias,
            dump = dump,
        )


    def _construct_select_query(self, *,
        base_table      : Table,
        join_tables     : List[Tuple[Table, JoinType]],
        join_ons        : Dict[Table, Tuple[str, list]],
        extra_columns   : List[Tuple[ExtraColumnExpr, ColumnAlias]],
        where_sql       : Optional[str],
        where_params    : list,
        groups          : List[Column],
        orders          : List[Tuple[Column, OrderType]],
        limit           : Optional[int],
        offset          : Optional[int],
        skip_same_alias : bool,
        dump            : bool = False,
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
        params:List[Any] = []

        join_on_used = {table: False for table in join_ons}
        for jointype, tlink in joins:
            _target_table = tlink.lcol.table
            sql += f' {self._fmt_jointype(jointype)} JOIN {_target_table.sql()} ON {tlink.lcol.sql()} = {tlink.rcol.sql()}'
            if join_ons.get(_target_table):
                _on_sql, _on_params = join_ons[_target_table]
                sql += ' AND (' + _on_sql + ')'
                params.extend(_on_params)
                join_on_used[_target_table] = True
            sql += '\n'
        if not all(join_on_used.values()):
            raise RuntimeError('Unused join_on setting(s) exist: %s' % ', '.join(table.name for table, used in join_on_used.items() if not used))

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

        if dump:
            print(sql, params, file=sys.stderr)

        # Return a tuple of (SQL statement, list of parameter values)
        return sql, params

    @staticmethod
    def _one_or_more(one:Optional[T], *mores:Optional[Iterable[T]]) -> Iterator[T]:
        if one is not None:
            yield one
        for more in mores:
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
