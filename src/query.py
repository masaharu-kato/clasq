"""
    SQL query module
"""

from itertools import chain
from typing import Any, List, Optional, Union, Tuple, NewType
from . import fmt
from . import schema

ColumnName = schema.ColumnName
TableName = schema.TableName
ColumnAs = Union[ColumnName, Tuple[ColumnName, str]]
JoinType = NewType('JoinType', str)
OrderType = NewType('OrderType', str)


class SQLQuery:
    """ SQL query class """
    def __init__(self, db:schema.Database):
        self.db = db
        
    def select_query(self, *,
        table         : Optional[TableName]                          = None,
        join_tables   : List[Tuple[TableName, JoinType]]             = None,
        tables        : Optional[List[TableName]]                    = None,
        opt_table     : Optional[TableName]                          = None,
        opt_tables    : Optional[List[TableName]]                    = None,
        extra_columns : Optional[List[str]]                          = None,
        where_sql     : Optional[str]                                = None,
        where_params  : Optional[list]                               = None,
        where_eq      : Optional[Tuple[ColumnName, Any]]             = None,
        where_eqs     : Optional[List[Tuple[ColumnName, Any]]]       = None,
        group         : Optional[ColumnName]                         = None,
        groups        : Optional[List[ColumnName]]                   = None,
        order         : Optional[Tuple[ColumnName, OrderType]]       = None,
        orders        : Optional[List[Tuple[ColumnName, OrderType]]] = None,
        limit         : Optional[int]                                = None,
        offset        : Optional[int]                                = None,
    ) -> Tuple[str, list]:
        """
            Calculate SQL SELECT query
            returns (SQL statement string, parameter values)    
        """

        if not table and not tables:
            raise RuntimeError('No table specified.')

        _join_tables = [*join_tables]
        if tables:
            _join_tables.extend((table, 'INNER') for table in (tables[1:] if table else tables)) # pylint: disable=superfluous-parens
        if opt_table:
            _join_tables.append((opt_table, 'LEFT'))
        if opt_tables:
            _join_tables.extend((table, 'LEFT') for table in opt_tables)

        _where_sqls = [where_sql] if where_sql else []
        _where_params = [*where_params]
        if where_eq:
            column, value = where_eq
            _where_sqls.append(f'{fmt.fmo(column)} = %s')
            _where_params.append(value)
        if where_eqs:
            for column, value in where_eqs:
                _where_sqls.append(f'{fmt.fmo(column)} = %s')
                _where_params.append(value)

        return self._select_query(
            base_table    = table if table else tables[0],
            join_tables   = _join_tables,
            extra_columns = extra_columns,
            where_sql     = ' AND '.join(_where_sqls),
            where_params  = _where_params,
            groups        = [group, *(groups or [])] if group else (groups or []),
            orders        = [order, *(orders or [])] if order else (orders or []),
            limit         = limit,
            offset        = offset,
        )


    def _select_query(self, *,
        base_table    : TableName,
        join_tables   : List[Tuple[TableName, JoinType]],
        extra_columns : List[str],
        where_sql     : Optional[str],
        where_params  : list,
        groups        : List[ColumnName],
        orders        : List[Tuple[ColumnName, OrderType]],
        limit         : Optional[int],
        offset        : Optional[int],
    ) -> Tuple[str, list]:

        columns = [*chain.from_iterable(self.db[table] for table in [base_table, *join_tables]), *extra_columns]

        joins:List[Tuple[JoinType, Tuple[TableName, Tuple[ColumnName, ColumnName]]]] = []
        _loaded_tables = [base_table]
        for target_table, jointype in join_tables:
            clink = self.db[target_table].find_tables_links(_loaded_tables)
            if clink is None:
                raise RuntimeError('No links found for the specified table.')
            joins.append((jointype, clink))
            _loaded_tables.insert(0, target_table)

        params = []

        sql = 'SELECT ' + ', '.join(map(fmt.fmo, columns)) + ' FROM ' + fmt.fmo(base_table) + '\n'

        for jointype, (table, (lcol, rcol)) in joins:
            sql += f' {fmt.jointype(jointype)} JOIN {fmt.fo(table)} ON {fmt.fmo(lcol)} = {fmt.fmo(rcol)}\n'

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


# class _SQLQuery:
#     @abstractmethod
#     def __sql__(self) -> str:
#         pass


# class SelectQuery(_SQLQuery):
#     def __init__(self, table:TableName, columns:List[ColumnAs], *, limit:Optional[int]=None, offset:Optional[int]=None):
#         self.table   : TableName = table
#         self.columns : List[ColumnName] = [c for c in columns]
#         self.joins   : List[Tuple[str, TableName, [ColumnName, ColumnName]]] = []
#         self._where  : Optional[str] = None
#         self.groups  : List[ColumnName] = []
#         self.orders  : List[Tuple[ColumnName, str]] = []
#         self.limit   : Optional[int] = limit
#         self.offset  : Optional[int] = offset

#     def join(self, jointype:str, table:TableName, columns:List[ColumnAs]):
#         self.joins.append((jointype, table, (table + '.id', table + '_id')))
#         self.columns.extend(columns)
#         return self

#     def inner_join(self, table:TableName, columns:List[ColumnAs]):
#         return self.join('INNER', table, columns)

#     def left_join(self, table:TableName, columns:List[ColumnAs]):
#         return self.join('LEFT', table, columns)

#     def right_join(self, table:TableName, columns:List[ColumnAs]):
#         return self.join('RIGHT', table, columns)

#     def where(self, cond:str):
#         if self._where is not None:
#             raise RuntimeError('where conditions are already set.')
#         self._where = cond
#         return self

#     def where_eq(self, column:ColumnName, value:str):
#         return self.where(fmt.fmo(column) + ' = ' + value)

#     def order_by(self, column:ColumnName, ordertype:str):
#         self.orders.append((column, ordertype))
#         return self

#     def order_asc_by(self, column:ColumnName):
#         return self.order_by(column, 'ASC')

#     def order_desc_by(self, column:ColumnName):
#         return self.order_by(column, 'DESC')

#     def group_by(self, column:ColumnName):
#         self.groups.append(column)
#         return self

#     def _sql_column_as(self, column:ColumnAs) -> str:

#         if isinstance(column, str):
#             return fmt.fmo(column)

#         if isinstance(column, (list, tuple)):
#             colname, colalias = column
#             return fmt.fmo(colname) + ' AS ' + _str(colalias)

#         raise TypeError('Unexpected type of column.')


#     def __sql__(self) -> str:

#         sql = 'SELECT ' + ', '.join(map(self._sql_column_as, self.columns)) + ' FROM ' + fmt.fo(self.table) + '\n'

#         for jointype, table, (lcol, rcol) in self.joins:
#             if not jointype in JOINTYPES:
#                 raise RuntimeError('Unknown join type `{}`'.format(jointype))
#             sql += ' ' + jointype + ' JOIN ' + fmt.fo(table) + ' ON ' + fmt.fmo(lcol) + ' = ' + fmt.fmo(rcol) + '\n'

#         if self.where:
#             sql += ' WHERE ' + self._where + '\n'

#         if self.groups:
#             sql += ' GROUP BY ' + ', '.join(map(fmt.fmo, self.groups)) + '\n'

#         if self.orders:
#             _order_sqls = []
#             for column, ordertype in self.orders:
#                 if not ordertype in ORDERTYPES:
#                     raise RuntimeError('Unknown order type `{}`'.format(ordertype))
#                 _order_sqls.append(fmt.fmo(column) + ' ' + ordertype)
#             sql += ' ORDER BY ' + ', '.join(_order_sqls) + '\n'

#         if self.limit is not None:
#             sql += ' LIMIT ' + int(self.limit) + '\n'

#         if self.offset is not None:
#             sql += ' OFFSET ' + int(self.offset) + '\n'

#         return sql
