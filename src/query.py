from abc import abstractmethod
from itertools import chain
from typing import Any, Dict, List, Optional, Union, Tuple, NewType
from .format import _fo, _fmo, _type, _str

Table  = NewType('Table', str)
Column = NewType('Column', str)
ColumnAs = Union[Column, Tuple[Column, str]]
JoinType = NewType('JoinType', str)
OrderType = NewType('OrderType', str)

JOINTYPES = ['INNER', 'RIGHT', 'LEFT']
ORDERTYPES = ['ASC', 'DESC']


def _ordertype(ordertype:OrderType) -> OrderType:
    if not ordertype in ORDERTYPES:
        raise RuntimeError('Invalid ordertype.')
    return ordertype


class SQLQuery:
    def __init__(self,
        table_columns:Dict[Table, List[Column]],
        table_links  :Dict[Table, Dict[Table, Column]],
    ):
        self.table_columns = table_columns
        self.table_links = table_links
        
    def select_query(self, *,
        table         : Optional[Table]                          = None,
        join_tables   : List[Tuple[Table, JoinType]]             = None,
        tables        : Optional[List[Table]]                    = None,
        opt_table     : Optional[Table]                          = None,
        opt_tables    : Optional[List[Table]]                    = None,
        extra_columns : Optional[List[str]]                      = None,
        where_sql     : Optional[str]                            = None,
        where_params  : Optional[list]                           = None,
        where_eq      : Optional[Tuple[Column, Any]]             = None,
        where_eqs_and : Optional[List[Tuple[Column, Any]]]       = None,
        group         : Optional[Column]                         = None,
        groups        : Optional[List[Column]]                   = None,
        order         : Optional[Tuple[Column, OrderType]]       = None,
        orders        : Optional[List[Tuple[Column, OrderType]]] = None,
        limit         : Optional[int]                            = None,
        offset        : Optional[int]                            = None,
    ) -> Tuple[str, list]:

        if not table and not tables:
            raise RuntimeError('No table specified.')

        _join_tables = [*join_tables]
        if tables:
            _join_tables.extend((table, 'INNER') for table in (tables[1:] if table else tables))
        if opt_table:
            _join_tables.append((opt_table, 'LEFT'))
        if opt_tables:
            _join_tables.extend((table, 'LEFT') for table in opt_tables)

        _where_sqls = [where_sql] if where_sql else []
        _where_params = [*where_params]
        if where_eq:
            column, value = where_eq
            _where_sqls.append(f'{_fmo(column)} = %s')
            _where_params.append(value)
        if where_eqs_and:
            for column, value in where_eqs_and:
                _where_sqls.append(f'{_fmo(column)} = %s')
                _where_params.append(value)

        return self._select_query(
            base_table    = table if table else tables[0],
            join_tables   = _join_tables,
            extra_columns = extra_columns,
            where_sql     = _where_sql,
            where_params  = _where_params,
            groups        = [group, *(groups or [])] if group else (groups or []),
            orders        = [order, *(orders or [])] if order else (orders or []),
            limit         = limit,
            offset        = offset,
        )


    def _select_query(self, *,
        base_table    : Table,
        join_tables   : List[Tuple[Table, JoinType]],
        extra_columns : List[str],
        where_sql     : Optional[str],
        where_params  : list,
        groups        : List[Column],
        orders        : List[Tuple[Column, OrderType]],
        limit         : Optional[int],
        offset        : Optional[int],
    ) -> Tuple[str, list]:

        columns = [*chain.from_iterable(self.table_columns[table] for table in chain(tables, opt_tables)), *extra_columns]

        joins:List[Tuple[JoinType, Tuple[Table, Tuple[Column, Column]]]] = []
        _loaded_tables = [base_table]
        for target_table, jointype in join_tables:
            clink = _find_table_link(_loaded_tables, target_table)
            if clink is None:
                raise RuntimeError('No links found for the specified table.')
            joins.append((jointype, clink))
            _loaded_tables.insert(0, table)

        params = []

        sql = 'SELECT ' + ', '.join(map(_fmo, columns)) + ' FROM ' + _fmo(base_table) + '\n'

        for jointype, (table, (lcol, rcol)) in joins:
            sql += f' {_jointype(jointype)} JOIN {_fo(table)} ON {_fmo(lcol)} = {fmo(rcol)}\n'

        if where_sql:
            sql += ' WHERE ' + where_sql + '\n'
            params.extend(where_params)

        if groups:
            sql += ' GROUP BY ' + ', '.join(map(_fmo, groups)) + '\n'

        if orders:
            sql += ' ORDER BY ' + ', '.join(f'{_fmo(column)} {_ordertype(ordertype)}' for column, ordertype in orders) + '\n'

        if limit is not None:
            sql += ' LIMIT %s\n'
            params.append(limit)

        if offset is not None:
            sql += ' OFFSET %s\n'
            params.append(offset)

        return sql, params


    def _find_table_link(self, tables:List[Table], dest_table:Table) -> Optional[Tuple[Table, Tuple[Column, Column]]]:
        for ctable in tables:
            clink = _get_table_link_with_rev(ttable, ctable)
            if clink is not None:
                return (ctable, clink)
        return None        

    def _get_table_link_with_rev(self, base_table:Table, dest_table:Table) -> Optional[Tuple[Column, Column]]:
        res = self._get_table_link(base_table, dest_table)
        if res is not None:
            return res
        return self._get_table_link(dest_table, base_table)

    def _get_table_link(self, base_table:Table, dest_table:Table) -> Optional[Tuple[Column, Column]]:
        if base_table in self.table_links and dest_table in self.table_links[base_table]:
            return (f'{dest_table}.id', f'{base_table}.{self.table_links[base_table][dest_table]}')
        return None


class _SQLQuery:
    @abstractmethod
    def __sql__(self) -> str:
        pass


class SelectQuery(_SQLQuery):
    def __init__(self, table:Table, columns:List[ColumnAs], *, limit:Optional[int]=None, offset:Optional[int]=None):
        self.table   : Table = table
        self.columns : List[Column] = [c for c in columns]
        self.joins   : List[Tuple[str, Table, [Column, Column]]] = []
        self._where  : Optional[str] = None
        self.groups  : List[Column] = []
        self.orders  : List[Tuple[Column, str]] = []
        self.limit   : Optional[int] = limit
        self.offset  : Optional[int] = offset

    def join(self, jointype:str, table:Table, columns:List[ColumnAs]):
        self.joins.append((jointype, table, (table + '.id', table + '_id')))
        self.columns.extend(columns)
        return self

    def inner_join(self, table:Table, columns:List[ColumnAs]):
        return self.join('INNER', table, columns)

    def left_join(self, table:Table, columns:List[ColumnAs]):
        return self.join('LEFT', table, columns)

    def right_join(self, table:Table, columns:List[ColumnAs]):
        return self.join('RIGHT', table, columns)

    def where(self, cond:str):
        if self._where is not None:
            raise RuntimeError('where conditions are already set.')
        self._where = cond
        return self

    def where_eq(self, column:Column, value:str):
        return self.where(_fmo(column) + ' = ' + value)

    def order_by(self, column:Column, ordertype:str):
        self.orders.append((column, ordertype))
        return self

    def order_asc_by(self, column:Column):
        return self.order_by(column, 'ASC')

    def order_desc_by(self, column:Column):
        return self.order_by(column, 'DESC')

    def group_by(self, column:Column):
        self.groups.append(column)
        return self

    def _sql_column_as(self, column:ColumnAs) -> str:

        if isinstance(column, str):
            return _fmo(column)

        if isinstance(column, (list, tuple)):
            colname, colalias = column
            return _fmo(colname) + ' AS ' + _str(colalias)

        raise TypeError('Unexpected type of column.')


    def __sql__(self) -> str:

        sql = 'SELECT ' + ', '.join(map(self._sql_column_as, self.columns)) + ' FROM ' + _fo(self.table) + '\n'

        for jointype, table, (lcol, rcol) in self.joins:
            if not jointype in JOINTYPES:
                raise RuntimeError('Unknown join type `{}`'.format(jointype))
            sql += ' ' + jointype + ' JOIN ' + _fo(table) + ' ON ' + _fmo(lcol) + ' = ' + _fmo(rcol) + '\n'

        if self.where:
            sql += ' WHERE ' + self._where + '\n'

        if self.groups:
            sql += ' GROUP BY ' + ', '.join(map(_fmo, self.groups)) + '\n'

        if self.orders:
            _order_sqls = []
            for column, ordertype in self.orders:
                if not ordertype in ORDERTYPES:
                    raise RuntimeError('Unknown order type `{}`'.format(ordertype))
                _order_sqls.append(_fmo(column) + ' ' + ordertype)
            sql += ' ORDER BY ' + ', '.join(_order_sqls) + '\n'

        if self.limit is not None:
            sql += ' LIMIT ' + int(self.limit) + '\n'

        if self.offset is not None:
            sql += ' OFFSET ' + int(self.offset) + '\n'

        return sql
    



