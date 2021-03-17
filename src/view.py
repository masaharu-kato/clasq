"""
    SQL query module
"""

from typing import Any, Dict, List, Sequence, Optional, Union, Tuple, NewType
from . import fmt
from . import schema
from . import sqlexpr as sqe
from . import executor
from itertools import chain


ColumnName = schema.ColumnName
ColumnLike = Union[schema.Column, ColumnName]
TableName = schema.TableName
TableLike = Union[schema.Table, TableName]
ColumnAs = Union[ColumnName, Tuple[ColumnName, str]]
JoinType = NewType('JoinType', str)
OrderType = NewType('OrderType', str)
COp = NewType('COp', str) # SQL comparison operator type


class DataView(sqe.SQLExprType):
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
        self.qe = qe
        self.db = db
        self._table = None
        self._joins = []
        self._join_parents = None
        self._colexprs = []
        self._terms = None
        self._groups = []
        self._orders = []
        self._limit = None
        self._offset = None

        self._result = None

        if table:
            self.table(table, full=full)

    def new(self, table:Optional[TableLike]=None, full:bool=True):
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

    def where(self, *exprs):
        """ Append terms """
        for expr in exprs:
            self._terms = expr if self._terms is None else self._terms & expr
        return self

    def column(self, *colexprs):
        """ Add extra column """
        self._colexprs.extend(colexprs)
        return self

    def group(self, *columns:ColumnLike):
        """ Append group(s) """
        self._groups.extend(columns)
        return self

    def orders(self, *colexprs):
        """ Append order(s) """
        self._orders.extend(colexprs)
        return self

    def limit(self, limit:Optional[int]):
        """ Set limit """
        self._limit = limit
        return self

    def offset(self, offset:Optional[int]):
        """ Set offset """
        self._offset = offset
        return self

    # def join_parents(self):
    #     self._join_parent_tables = True
    #     return self

    # def join_children(self):
    #     self._join_child_tables = True
    #     return self


    def term(self, l, op, r):
        lexpr = l if isinstance(l, sqe.SQLExprType) else self.db.column(l)
        return self.where(sqe.BinaryExpr(op, lexpr, r))

    def eq(self, table:TableLike, **kwargs):
        """ Append equal terms on WHERE clause """
        table = self.db.table(table)
        for column_name, value in kwargs.items():
            self.where(table.column(column_name) == value)
        return self
        
    def id(self, idval:int):
        """ Append `id` equal term on WHERE clause """
        return self.eq(self._table, id=idval)


    def __getitem__(self, key):
        """ Append various terms """
        if isinstance(key, sqe.SQLExprType):
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

    def execute(self):
        self._result = self.qe.query(self.sql_with_params)
        return self

    def prepare(self):
        if self._result is None:
            self.execute()
        return self

    def refresh(self):
        return self.execute()

    @property
    def result(self):
        self.prepare()
        return self._result

    def __iter__(self):
        return iter(self.result)

    def __len__(self):
        return len(self.result)

    def one(self):
        self.prepare()
        if len(self._result) != 1:
            raise RuntimeError('Length of result is not just one.')
        return self._result[0]

    def one_or_none(self):
        self.prepare()
        if self._result:
            return self.one()
        return None

    def exists(self):
        return bool(self.__len__())


    def pages(self) -> List['DataView']:
        # TODO: Implementation
        raise NotImplementedError()

    def __sqlout__(self, swp:sqe.SQLWithParams) -> None:

        print('id(swp)=', id(swp))

        if not self._table:
            raise RuntimeError('Table is not set.')

        # clarify target columns
        target_columns = [*self._table.columns]
        for table, _ in self._joins:
            target_columns.extend(table.columns)
        for colexpr in self._colexprs:
            target_columns.append(self.db.column(colexpr))

        # construct query
        swp.append_clause('SELECT', [sqe.col_opt_as(col, col.opt_unique_alias()) for col in target_columns])
        swp.append_clause('FROM'  , self._table)

        for table, (lcol, rcol) in self._joins:
            swp.append_clause(('INNER' if lcol.not_null and rcol.not_null else 'LEFT') + ' JOIN', table, end='')
            swp.append_clause('ON', lcol == rcol)

        swp.append_clause('WHERE'   , self._terms)
        swp.append_clause('GROUP BY', self._groups)
        swp.append_clause('ORDER BY', [sqe.ordered_column(order) for order in self._orders])
        swp.append_clause('LIMIT'   , self._limit)
        swp.append_clause('OFFSET'  , self._offset)
        
        #print(swp.sql, 'params:', swp.params)
        #print('')

    @property
    def sql_with_params(self, default_swp:Optional[sqe.SQLWithParams]=None) -> sqe.SQLWithParams:
        swp = sqe.SQLWithParams() if default_swp is None else default_swp
        # print('generated id(swp)=', id(swp))
        swp.append(self)
        # print(swp.sql, swp.params)
        return swp


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

    def __getattr__(self, tablename:str):
        return self.dv.table(tablename)
