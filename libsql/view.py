"""
    SQL query module
"""

from typing import Any, Dict, List, Sequence, Optional, Union, Tuple, NewType

from . import schema
from .syntax import sql_expression as sqlex
from . import executor

ColumnName = schema.ColumnName
ColumnLike = Union[schema.Column, ColumnName]
TableName = schema.TableName
TableLike = Union[schema.Table, TableName]
ColumnAs = Union[ColumnName, Tuple[ColumnName, str]]
JoinType = NewType('JoinType', str)
OrderType = NewType('OrderType', str)
COp = NewType('COp', str) # SQL comparison operator type

class DataView(sqlex.SQLExprType):
    """ Data-view class
        e.g. 
        dv = DataView(...)

        # basic
        list(dv.new('students')) # list all student records
        list(dv.new('students').eq('students', age=18)) # list students whose age is 18
        dv.new('students').id(123).one() # get a student whose id is 123

        # orders
        list(dv.new('students').order(+name, -age)) # list students with order of name ASC, age DESC
        list(dv.new('students')[+name, -age]) # same

        # groups
        list(dv.new('students').group('age')) # list of count of students grouped by `age` column

        # offset and limit
        dv.new('students')[100:] # list students (offset = 100)
        dv.new('students')[100:150] # list students (offset = 100, limit = 50)
        dv.new('students')[100::50] # list students (offset = 100, limit = 50)
    """

    # def __init__(self, db:schema.Database, table:Optional[TableLike]=None, *, parent:bool=True, child:bool=True)

    def __init__(self,
        qe:executor.BasicQueryExecutor,
        db:schema.Database,
        table:Optional[TableLike]=None,
        full:bool=True
    ):
        self.qe : executor.BasicQueryExecutor = qe
        self.db : schema.Database = db

        self._table    : Optional[TableLike]                    = None
        self._joins    : List[TableLike]                    = []
        self._colexprs : Sequence[sqlex.SQLExprType]          = []
        self._terms    : Optional[sqlex.SQLExprType]          = None
        self._groups   : Sequence[Union[ColumnLike, TableLike]] = []
        self._orders   : List[sqlex.SQLExprType]          = []
        self._limit    : Optional[int]                          = None
        self._offset   : Optional[int]                          = None

        self._result = None

        if table:
            self.table(table, full=full)

    def new(self, table:Optional[TableLike]=None, *, full:bool=True):
        """ Generate a new view instance with table """
        return DataView(self.qe, self.db, table, full=full)

    def table(self, table:TableLike, *, full:bool=True):
        """ Set a base table """
        self._table = self.db.table(table)
        if full:
            self.join(*self._table.get_parent_table_links())
        return self

    @property
    def tables(self):
        return DataViewTables(self)

    def join(self, *tables:TableLike):
        """ Join other tables """
        for table in tables:
            self._joins.append(self.db.table(table))
        return self

    def join_new(self, *tables:TableLike):
        """ Generate a new view instance with table joined with other tables """
        return self.new(self._table, *self._joins, *tables)

    def where(self, *exprs) -> 'DataView':
        """ Append terms """
        for expr in exprs:
            self._terms = expr if self._terms is None else self._terms & expr
        return self

    def column(self, *colexprs) -> 'DataView':
        """ Add extra column """
        self._colexprs.extend(colexprs)
        return self

    def group(self, *columns:Union[ColumnLike, TableLike]) -> 'DataView':
        """ Append group column(s) or table(s) """
        self._groups.extend(columns)
        return self

    def orders(self, *colexprs) -> 'DataView':
        """ Append order(s) """
        self._orders.extend(colexprs)
        return self

    def limit(self, limit:Optional[int]) -> 'DataView':
        """ Set limit """
        self._limit = limit
        return self

    def offset(self, offset:Optional[int]) -> 'DataView':
        """ Set offset """
        self._offset = offset
        return self

    # def join_parents(self):
    #     self._join_parent_tables = True
    #     return self

    # def join_children(self):
    #     self._join_child_tables = True
    #     return self

    def term(self, l, op, r) -> 'DataView':
        """ Append term for WHERE clause """
        lexpr = l if isinstance(l, sqlex.SQLExprType) else self.db.column(l)
        return self.where(sqlex.BinaryExpr(op, lexpr, r))

    def eq(self, table:TableLike, **kwargs) -> 'DataView':
        """ Append equal terms on WHERE clause """
        table = self.db.table(table)
        for column_name, value in kwargs.items():
            self.where(table.column(column_name) == value)
        return self
        
    def id(self, idval:int):
        """ Append `id` equal term on WHERE clause """
        assert self._table is None
        return self.eq(self._table, id=idval)


    def __getitem__(self, key):
        """ Append various terms """
        if isinstance(key, sqlex.SQLExprType):
            return self.where(key)

        if isinstance(key, slice):
            if key.start is not None:
                self.offset(key.start)
            if key.stop is not None:
                self.limit(key.stop - (key.start or 0))
            if key.step is not None:
                self.limit(key.step)
            return self

        if isinstance(key, int): # id value
            return self.id(key)

        raise TypeError('Unexpected type.')

    def execute(self) -> 'DataView':
        """ Execute SQL """
        self._result = self.qe.query(self.sql_with_params())
        return self

    def prepare(self) -> 'DataView':
        """ Prepare result (Execute SQL if not executed yet) """
        if self._result is None:
            self.execute()
        return self

    def refresh(self) -> 'DataView':
        """ Re-execute SQL """
        return self.execute()

    @property
    def result(self):
        """ Get result """
        self.prepare()
        return self._result

    def __iter__(self):
        """ Iterate the result """
        return iter(self.result)

    def __len__(self):
        """ Get length of the result """
        return len(self.result)

    def one(self):
        """ Assume one result and get it """
        self.prepare()
        if len(self._result) != 1:
            raise RuntimeError('Length of result is not just one.')
        return self._result[0]

    def one_or_none(self):
        """ Assume one result or None and get it """
        self.prepare()
        if self._result:
            return self.one()
        return None

    def exists(self):
        """ Get existence of result """
        return bool(self.__len__())


    def pages(self) -> List['DataView']:
        # TODO: Implementation
        raise NotImplementedError()


    def sql_with_params(self) -> Tuple[str, list]:
        """ Generate SQL and parameters to SQLWithParams object """

        if self._table is None:
            raise RuntimeError('Table is not set.')

        # clarify target columns
        target_columns = [*self._table.columns]
        for table, _ in self._joins:
            target_columns.extend(self.db[table].columns)
        for colexpr in self._colexprs:
            assert isinstance(colexpr, (schema.Column, str)) 
            target_columns.append(self.db.column(colexpr))

        sql, params = '', []

        def append_clause(clause_name:str, *vals, end:str='\n'):
            """ Append new clause """
            nonlocal sql, params
            if vals[0] is None or (isinstance(vals[0], (tuple, list)) and not vals[0]):
                return self
            csql, cparams = sqlex.sql_with_params(sqlex.RawExpr(clause_name), *vals, end=end)
            sql += csql
            params.extend(cparams)

        # construct query
        append_clause('SELECT', [(col, sqlex.RawExpr('AS'), sqlex.QuotedName(col.unique_alias())) for col in target_columns])
        append_clause('FROM'  , self._table)

        for table, (lcol, rcol) in self._joins:
            append_clause(('INNER' if lcol.not_null and rcol.not_null else 'LEFT') + ' JOIN', table, end=' ')
            append_clause('ON', lcol == rcol)

        append_clause('WHERE'   , self._terms)
        append_clause('GROUP BY', self._groups)
        append_clause('ORDER BY', [
            (colexpr, sqlex.RawExpr('DESC' if isinstance(colexpr, sqlex.UnaryExpr) and colexpr.op == '-' else 'ASC'))
            for colexpr in self._orders
        ])
        append_clause('LIMIT'   , self._limit)
        append_clause('OFFSET'  , self._offset)
        
        return sql, params


# @staticmethod
# def AUTO_ALIAS_FUNC(tablename:str, colname:str) -> str:
#     """ Automatically aliasing function for columns expressions in SELECT query """
#     return tablename[:-1] + '_' + colname


# def assert_type(value, *types):
#     """
#         Assert type of specified value
#         Specify each element of `types` to tuple of types to union type.
#         Specify `types` to the types on iterate levels.
#         e.g. list/tuple of tuple of 
#     """
#     if isinstance(, type):
#         pass


class DataViewTables:
    def __init__(self, dv:DataView):
        self.dv = dv

    def __getattr__(self, tablename:Union[str, TableName]):
        return self.dv.table(TableName(tablename))
