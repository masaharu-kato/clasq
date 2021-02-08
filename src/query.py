"""
    SQL query module
"""

from typing import Any, Dict, List, Optional, Union, Tuple, NewType
from . import fmt
from . import schema

ColumnName = schema.ColumnName
TableName = schema.TableName
ColumnAs = Union[ColumnName, Tuple[ColumnName, str]]
JoinType = NewType('JoinType', str)
OrderType = NewType('OrderType', str)
COp = NewType('COp', str) # SQL comparison operator type

COMP_OPS = ['=', 'IS', 'IS NOT', '<', '<=', '<=>', '>', '>=', '!=', '<>', 'BETWEEN', 'NOT BETWEEN', 'IN', 'NOT IN', 'LIKE', 'NOT LIKE', 'AND', 'OR']
COMP_OP_ALIASES = {'==':'=', 'INSIDE': 'BETWEEN', 'OUTSIDE': 'NOT BETWEEN'}


class QueryMaker:
    """ SQL query maker """

    def __init__(self, db:schema.Database):
        self.db = db
        

    def select(self,
        table         : Optional[TableName]                          = None , # base table name (if `table` is not specified, 1st element of `tables` will be a base table)
        *,
        tables        : Optional[List[TableName]]                    = None , # tables for INNER JOIN
        opt_table     : Optional[TableName]                          = None , # table for LEFT (optional) JOIN
        opt_tables    : Optional[List[TableName]]                    = None , # tables for LEFT (optional) JOIN
        join_tables   : List[Tuple[TableName, JoinType]]             = None , # tables for JOIN with JOIN type (INNER, LEFT, RIGHT)
        extra_columns : Optional[List[Tuple[str, str]]]              = None , # extra column exprs
        where_sql     : Optional[str]                                = None , # SQL of WHERE part
        where_params  : Optional[list]                               = None , # Parameters for WHERE part
        where_eq      : Optional[Tuple[ColumnName, Any]]             = None , # WHERE condition with ColumnName = Any value (if the value is list or tuple, `IN` statement is used)
        where_eqs     : Optional[List[Tuple[ColumnName, Any]]]       = None , # WHERE AND conditions with ColumnName = value (if the value is list or tuple, `IN` statement is used)
        where_op      : Optional[Tuple[ColumnName, COp, Any]]        = None , # WHERE condition with ColumnName, Comparison Operator('='/'==', '<', '<=', 'LIKE', etc.), value 
        where_ops     : Optional[List[Tuple[ColumnName, COp, Any]]]  = None , # WHERE AND conditions with ColumnName, Comparison Operator('='/'==', '<', '<=', 'LIKE', etc.), value 
        id            : Optional[int]                                = None , # value of base table primary key (for WHERE condition)
        group         : Optional[ColumnName]                         = None , # column expr for GROUP BY
        groups        : Optional[List[ColumnName]]                   = None , # column exprs for GROUP BY
        order         : Optional[Tuple[ColumnName, OrderType]]       = None , # order (column name and order type:DESC or ASC) for ORDER BY
        orders        : Optional[List[Tuple[ColumnName, OrderType]]] = None , # orders (column name and order type:DESC or ASC) for ORDER BY
        limit         : Optional[int]                                = None , # LIMIT value
        offset        : Optional[int]                                = None , # OFFSET value
        parent_tables : bool                                         = False, # join all parent tables of base table
        child_tables  : bool                                         = False, # join all child tables of base table
        skip_duplicate_columns: bool                                 = True , # skip error on duplicate column names
    ) -> Tuple[str, list]: # (SQL statement string, list of parameter values)
        """
            Build SQL SELECT query
        """

        # Set base table
        if not table and not tables:
            raise RuntimeError('No table specified.')
        base_table = table if table else tables[0]

        # Process table(s) and opt_table(s)
        _join_tables = [*join_tables] if join_tables else []
        if not all(isinstance(jt, tuple) and len(jt) == 2 for jt in _join_tables):
            raise TypeError('Invalid form of join_tables.')
        if tables:
            _join_tables.extend((table, 'INNER') for table in (tables if table else tables[1:])) # pylint: disable=superfluous-parens
        if opt_table:
            _join_tables.append((opt_table, 'LEFT'))
        if opt_tables:
            _join_tables.extend((table, 'LEFT') for table in opt_tables)

        # Join parent tables or/and child tables
        if parent_tables:
            _join_tables.extend((table, 'INNER' if not_null else 'LEFT') for table, not_null in self.db[base_table].get_parent_tables_recursively())
        if child_tables:
            _join_tables.extend((table, 'INNER' if not_null else 'LEFT') for table, not_null in self.db[base_table].get_child_tables_recursively())

        # Add extra columns
        if extra_columns and not all(isinstance(ec, tuple) and len(ec) == 2 for ec in extra_columns):
            raise TypeError('Invalid form of extra columns')

        # Process WHERE
        _where_sqls = [where_sql] if where_sql else []
        _where_params = [] if where_params is None else [*where_params]

        _where_ops = []

        if id is not None:
            _where_ops.append((f'{base_table}.id', '=', id)) # Add base table primary key value

        if where_eq is not None:
            col, val = where_eq # serves as a type check
            _where_ops.append((col, '=', val))

        for col, val in (where_eqs or []): # serves as a type check
            _where_ops.append((col, '=', val))

        if where_op is not None:
            col, op, val = where_op # serves as a type check
            _where_ops.append((col, op, val))

        for col, op, val in (where_ops or []): # serves as a type check
            _where_ops.append((col, op, val))
        
        for column, _op, value in _where_ops:
            op = _op.upper()
            op = COMP_OP_ALIASES.get(op) or op
            if op not in COMP_OPS:
                raise RuntimeError('Unknown operator `{}`.'.format(op))

            lval = fmt.fmo(column)
            if op == 'IN' or (op == '=' and isinstance(value, (list, tuple))):
                _where_sqls.append(lval + ' IN (' + ', '.join('%s' for _ in range(len(value))) + ')')
                _where_params.extend(value)
            elif op in ('BETWEEN', 'NOT BETWEEN'):
                first, last = value
                _where_sqls.append(lval + op + ' %s AND %s')
                _where_params.extend([first, last])
            else:
                _where_sqls.append(lval + ' ' + op + ' %s')
                _where_params.append(value)
            
        # Build statement
        return self._select(
            base_table    = base_table,
            join_tables   = _join_tables,
            extra_columns = [] if extra_columns is None else extra_columns,
            where_sql     = ' AND '.join(_where_sqls),
            where_params  = _where_params,
            groups        = [group, *(groups or [])] if group else (groups or []),
            orders        = [order, *(orders or [])] if order else (orders or []),
            limit         = limit,
            offset        = offset,
            skip_duplicate_columns = skip_duplicate_columns,
        )


    def _select(self, *,
        base_table    : TableName,
        join_tables   : List[Tuple[TableName, JoinType]],
        extra_columns : List[Tuple[str, str]],
        where_sql     : Optional[str],
        where_params  : list,
        groups        : List[ColumnName],
        orders        : List[Tuple[ColumnName, OrderType]],
        limit         : Optional[int],
        offset        : Optional[int],
        skip_duplicate_columns : bool,
    ) -> Tuple[str, list]:

        column_exprs:Dict[str, Union[List[schema.Column, bool], str]] = {}
        for column in self.db[base_table].columns:
            column_exprs[column.name] = [column, False] # output table : no

        for table, _ in join_tables:
            for column in self.db[table].columns:
                if not column.name in column_exprs:
                    column_exprs[column.name] = [column, False]
                else:
                    column_exprs[column.name][1] = True  # output table : yes
                    _alias = self.AUTO_ALIAS_FUNC(column.table.name, column.name)
                    if _alias in column_exprs:
                        if not skip_duplicate_columns:
                            raise RuntimeError('Alias `{}` already used.'.format(_alias))
                    else:
                        column_exprs[_alias] = [column, True]

        for column_expr, alias in extra_columns:
            if alias in column_exprs:
                raise RuntimeError('Alias `{}` already used.'.format(_alias))
            column_exprs[alias] = f'({column_expr})'

        joins:List[Tuple[JoinType, Tuple[ColumnName, ColumnName]]] = []
        _loaded_tables = [base_table]
        for target_table, jointype in join_tables:
            clink = list(self.db[target_table].find_tables_links(_loaded_tables))
            if not len(clink) == 1:
                raise RuntimeError('No links or multiple links found between table `{}` from tables {}.'.format(target_table, ', '.join('`{}`'.format(t) for t in _loaded_tables)))
            joins.append((jointype, clink[0]))
            _loaded_tables.insert(0, target_table)

        params = []

        col_sqls = []
        for alias, column_expr in column_exprs.items():
            if isinstance(column_expr, (tuple, list)):
                column, f_output_table = column_expr
                if not f_output_table:
                    col_sqls.append(fmt.fo(column))
                else:
                    _expr = fmt.fo(column.table) + '.' + fmt.fo(column)
                    if str(column) == alias:
                        col_sqls.append(_expr)
                    else:
                        col_sqls.append(_expr + ' AS ' + fmt.strval(alias))
            else:
                col_sqls.append(column_expr + ' AS ' + fmt.strval(alias))

        sql = 'SELECT ' + ', '.join(col_sqls) + ' FROM ' + fmt.fmo(base_table) + '\n'

        for jointype, (lcol, rcol) in joins:
            sql += f' {fmt.jointype(jointype)} JOIN {fmt.fo(rcol.table)} ON {fmt.fo(lcol.table)}.{fmt.fo(lcol)} = {fmt.fo(rcol.table)}.{fmt.fo(rcol)}\n'

        if where_sql:
            sql += ' WHERE ' + where_sql + '\n'
            params.extend(where_params)

        if groups:
            sql += ' GROUP BY ' + ', '.join(map(fmt.fmo, groups)) + '\n'

        if orders:
            sql += ' ORDER BY ' + ', '.join(f'{fmt.fmo(column)} {fmt.ordertype(ordertype)}' for column, ordertype in orders) + '\n'

        if limit is not None:
            sql += ' LIMIT %s\n'
            params.append(limit)

        if offset is not None:
            sql += ' OFFSET %s\n'
            params.append(offset)

        return sql, params



    @staticmethod
    def AUTO_ALIAS_FUNC(tablename:str, colname:str) -> str:
        """ Automatically aliasing function for columns expressions in SELECT query """
        return tablename[:-1] + '_' + colname


    # def assert_type(value, *types):
    #     """
    #         Assert type of specified value
    #         Specify each element of `types` to tuple of types to union type.
    #         Specify `types` to the types on iterate levels.
    #         e.g. list/tuple of tuple of 
    #     """
    #     if isinstance(, type):
    #         pass
