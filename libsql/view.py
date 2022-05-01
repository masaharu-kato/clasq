"""
    SQL query module
"""

from abc import abstractmethod
from typing import Iterable, Iterator, List, NamedTuple, NewType, Optional, Sequence, Tuple, Union

from .syntax.keywords import JoinType
from .syntax.expr_type import ExprType
from .syntax.schema_expr import ViewExpr, TableExpr, ColumnExpr, OrderedColumnExpr

class View(ViewExpr):
    """ Table View """
    def __init__(self):
        super().__init__()
        self._select_exprs: List[ExprType] = []
        self._from_tables : List[TableExpr] = []
        self._joins  : List[Tuple[TableExpr, JoinType, ExprType]] = []
        self._where  : Optional[ExprType] = None
        self._groups : List[ColumnExpr] = []
        self._orders : List[OrderedColumnExpr] = []
        self._limit  : Optional[int] = None
        self._offset : Optional[int] = None

    # ----------------------------------------------------------------
    #   Query methods
    # ----------------------------------------------------------------

    def fetch(self):
        """ Fetch a single result row """
        # TODO: Implement

    def fetchall(self):
        """ Fetch all reuslt rows """
        # TODO: Implement

    def select(self, *args, **kwargs):
        """ Run SELECT query """
        self.db_cursor().select(*args, **kwargs) # TODO: Implement

    def insert(self, *args, **kwargs):
        """ Run INSERT query """
        self.db_cursor().insert(*args, **kwargs) # TODO: Implement

    def insertmany(self, *args, **kwargs):
        """ Run multiple INSERT queries """
        self.db_cursor().insertmany(*args, **kwargs) # TODO: Implement

    def update(self, *args, **kwargs):
        """ Run UPDATE query """
        self.db_cursor().update(*args, **kwargs) # TODO: Implement

    def delete(self, *args, **kwargs):
        """ Run DELETE query """
        self.db_cursor().delete(*args, **kwargs) # TODO: Implement

    def selsert(self, *args, **kwargs):
        """ Run SELECT or INSERT query """
        self.db_cursor().selsert(*args, **kwargs) # TODO: Implement

    def upsert(self, *args, **kwargs):
        """ Run UPDATE or INSERT query """
        self.db_cursor().upsert(*args, **kwargs) # TODO: Implement


    # ----------------------------------------------------------------
    #   View creation methods
    # ----------------------------------------------------------------

    def select_column(self, *ops, **kwargs) -> 'TableColumnsView':
        """ Make a View object with columns """

    def where(self, *ops, **kwargs) -> 'WheredView':
        """ Make a View object with WHERE clause """
        return WheredView(self, []) # TODO: Implement

    def __call__(self, *ops, **kwargs):
        """ Make a View object with WHERE clause (alias of `where` method) """
        return self.where(*ops, **kwargs)

    def key(self, *ops, **kwargs) -> 'WheredView':
        """ Make a View object with WHERE clause (Returns a unique row) """
        return WheredView(self, []) # TODO: Implement

    def group_by(self, *columns: ViewColumnABC) -> 'GroupedView':
        return GroupedView(self, columns) # TODO: Implement

    def order_by(self, *orders: SQLExprType) -> 'OrderedView':
        """ Make a View object with ORDER BY clause """
        return OrderedView(self, orders) # TODO: Implement

    def limit(self, limit: int) -> 'LimitedView':
        """ Make a View object with LIMIT OFFSET clause """
        return LimitedView(self, limit)

    def offset(self, offset: int) -> 'OffsetView':
        """ Make a View object with LIMIT OFFSET clause """
        return OffsetView(self, offset)

    def single(self) -> 'SingleView':
        """ Make a View object with a single result """
        return SingleView(self)

    def join(self, type: Join, factor: 'ViewABC', cond: SQLExprType, **cond_eqs: ViewColumnABC):
        """ Make a Joined View """
        # TODO: cond_eqs
        return JoinedView(type, self, factor, cond)

    def sql(self) -> str:
        """ Generate SQL string """
        return self.table_sql()

    def inner_join(self, factor: 'ViewABC', cond: SQLExprType, **cond_eqs: ViewColumnABC):
        return self.join(Join.INNER, factor, cond, **cond_eqs)

    def left_join(self, factor: 'ViewABC', cond: SQLExprType, **cond_eqs: ViewColumnABC):
        return self.join(Join.LEFT, factor, cond, **cond_eqs)

    def right_join(self, factor: 'ViewABC', cond: SQLExprType, **cond_eqs: ViewColumnABC):
        return self.join(Join.RIGHT, factor, cond, **cond_eqs)

    def outer_join(self, factor: 'ViewABC', cond: SQLExprType, **cond_eqs: ViewColumnABC):
        return self.join(Join.OUTER, factor, cond, **cond_eqs)

    def cross_join(self, factor: 'ViewABC', cond: SQLExprType, **cond_eqs: ViewColumnABC):
        return self.join(Join.CROSS, factor, cond, **cond_eqs)


    # ----------------------------------------------------------------
    #   System view methods
    # ----------------------------------------------------------------

    @property
    def table_view(self) -> 'TableViewABC':
        return self

    @abstractmethod
    def table_sql(self) -> str:
        """ Get SQL of Table """

    @property
    def where_cond(self) -> SQLExprType:
        """ Get WHERE conds """
        return None # Default Implementation

    @abstractmethod
    def iter_orders(self) -> Iterator[OrderedViewColumn]:
        """ Iterate ORDER columns """


    # ----------------------------------------------------------------
    #   System column methods
    # ----------------------------------------------------------------

    @property
    def columns(self) -> List[ViewColumnABC]:
        """ Get all columns """
        return list(self.iter_columns())

    @abstractmethod
    def iter_columns(self) -> Iterator[ViewColumnABC]:
        """ Iterate columns """

    @abstractmethod
    def column(self, column: ColumnLike) -> ViewColumnABC:
        """ Search a specific column """

    def col(self, column: ColumnLike) -> ViewColumnABC:
        """ Search a specific column (alias of `self.column()`) """
        return self.column(column)

    # def __getitem__(self, column: ColumnLike) -> ViewColumnABC:
    #     """ Search a specific column (alias of `self.column()`) """
    #     return self.column(column)

    
class SingleView(ViewABC):
    """ Single result view """

    def __init__(self, view: GroupedViewABC) -> None:
        super().__init__()
        self.view = view

    def where(self, *args, **kwargs):
        """ (override) """
        return Record() # TODO: Implement


class Record():
    """ Record Object """

    # ----------------------------------------------------------------
    #   Data methods
    # ----------------------------------------------------------------

    @property
    def data(self) -> Optional[NamedTuple]:
        """ Get a data (Named-tuple of column values) """

    def __getitem__(self, column):
        """ Get a value of specific column """

    def __bool__(self):
        """ Return whether the actual data exists or not """


    # ----------------------------------------------------------------
    #   Query methods
    # ----------------------------------------------------------------

    def update(self, *args, **kwargs):
        """ Run UPDATE query """

    def delete(self):
        """ Run DELETE query """


