"""
    SQL query module
"""

from abc import abstractmethod, abstractproperty
from enum import Enum
from typing import Iterable, Iterator, List, NamedTuple, NewType, Optional, Sequence, Union

from .connector import ConnectionABC, CursorABC
from .meta import SQLObjABC, tosql
from .syntax.format import astext
from .syntax.sql_expression import SQLExprType


TableName  = NewType('TableName', str)
ColumnName = NewType('ColumnName', str)



class Join(Enum):
    """ Table JOIN types """
    NONE = None
    INNER = 'INNER'
    LEFT  = 'LEFT'
    RIGHT = 'RIGHT'
    OUTER = 'OUTER'
    CROSS = 'CROSS'


class Order(Enum):
    """ Table Order types """
    NONE = None
    ASC  = 'ASC'
    DESC = 'DESC'


class Where(Enum):
    """ Where operations """
    NULL = None
    NOT_NULL = 'NOT_NULL'
    EVAL_TRUE = 'EVAL_TRUE'
    EVAL_FALSE = 'EVAL_FALSE'
    EVAL_FALSE_OR_NULL = 'EVAL_FALSE_OR_NULL'


JoinLike = Union[Join, str]
OrderLike = Union[Order, str]


class ViewColumnABC(SQLObjABC):
    """ Column on View """

    @property
    def alias(self) -> Optional[str]:
        """ Get alias stirng """
        return None

ColumnLike = Union[ViewColumnABC, ColumnName, str]


class ExtraViewColumn(ViewColumnABC):
    """ Extra Column on View """
    def __init__(self, expr: SQLExprType):
        super().__init__()
        self._expr = expr

    def sql(self) -> str:
        return tosql(self._expr)

class AliasedViewColumn(ViewColumnABC):
    """ Column with alias """
    def __init__(self, column: ViewColumnABC, alias: Optional[str] = None):
        super().__init__()
        assert isinstance(column, ViewColumnABC), "Invalid type of column."
        self.column = column
        self._alias = alias

    @property
    def alias(self) -> str:
        return self._alias

    def sql(self) -> str:
        col_sql = self.column.sql()
        if self.alias:
            return col_sql + " AS " + astext(self.alias)
        return col_sql


class OrderedViewColumn(SQLObjABC):
    """ Column with Order """
    def __init__(self, column: ViewColumnABC, order: Order) -> None:
        super().__init__()
        self.column = column
        self.order = order


class ViewABC(SQLObjABC):
    """ Table View """

    # ----------------------------------------------------------------
    #   Database connection methods
    # ----------------------------------------------------------------
    @classmethod
    def create_db_connection(cls):
        """ Create a new Database Connection """
        # TODO: Implement

    @classmethod
    def create_db_cursor(cls):
        """ Create a new Database Cursor """

    @classmethod
    def db_connection(cls) -> ConnectionABC:
        """ Get the Database Connection """
        # TODO: Implement

    @classmethod
    def db_cursor(cls) -> CursorABC:
        """ Get the Database Cursor """

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

    



TableLike = Union[ViewABC, TableName, str]


class LimitedViewABC(ViewABC):
    """ Limited View ABC """

class OrderedViewABC(LimitedViewABC):
    """ Ordered View ABC """

class GroupedViewABC(OrderedViewABC):
    """ Grouped View ABC """

class WheredViewABC(GroupedViewABC):
    """ Whered View ABC """

class TableViewABC(WheredViewABC):
    """ Table View ABC """


class TableColumnsView(TableViewABC):
    """ Table view with columns """
    def __init__(self,
        view: ViewABC,
        columns: Union[None, bool, Sequence[Union[ColumnLike, ViewColumnABC]]] = True,
        **aliased_columns: Union[bool, ColumnLike, SQLExprType]
    ):
        super().__init__()
        self._view = view
        self._columns: List[ViewColumnABC] = []

        # If `columns` is True (All columns), extend all columns except specific columns set by `aliased_columns`
        if columns is True:
            self._columns.extend(column for column in view.iter_columns() if aliased_columns.get(column.name, True))

        # If `columns` is a list, extend specified columns
        elif columns is not None and columns is not False:
            self._columns.extend(column if isinstance(column, ViewColumnABC) else view.column(column) for column in columns)

        for alias, column in aliased_columns:
            if column is True:
                self._columns.append(ViewColumnABC(view.column(alias), alias))
            elif column is not False:
                self._columns.append(ExtraViewColumn(column, alias) if isinstance(column, SQLExprType) else AliasedViewColumn(view.column(column), alias))

    def iter_columns(self) -> Iterator[ViewColumnABC]:
        yield from self._columns


class JoinedView(TableViewABC):
    """ Joined view (view with JOIN clause) """

    def __init__(self, type: Join, origin: ViewABC, factor: ViewABC, cond: SQLExprType):
        super().__init__()
        self.type = type
        self.origin = origin
        self.factor = factor
        self.cond = cond

    def iter_columns(self) -> Iterator[ViewColumnABC]:
        yield from self.origin.table_view.iter_columns()
        yield from self.factor.table_view.iter_columns()

    @property
    def where_cond(self) -> SQLExprType:
        return self.origin.where_cond & self.factor.where_cond

    def iter_orders(self) -> Iterator[OrderedViewColumn]:
        yield from self.origin.iter_orders()
        yield from self.factor.iter_orders()

    def table_sql(self):
        return '%s %s JOIN %s ON %s' % (self.origin.table_view.table_sql(), self.type.name, self.factor.table_view.table_sql(), tosql(self.cond))


class WheredView(WheredViewABC):
    """ View with WHERE clause """

    def __init__(self, view: OrderedViewABC, cond: SQLExprType):
        super().__init__()
        assert isinstance(view, OrderedViewABC), "Invalid type of view."
        self.view = view
        self.cond = cond

    @property
    def table_view(self) -> ViewABC:
        return self.view

    @property
    def where_cond(self) -> SQLExprType:
        return self.view.where_cond & self.cond


class GroupedView(GroupedViewABC):
    """ View with GROUP BY clause """

    def __init__(self, view: OrderedViewABC, columns: Iterable[ViewColumnABC]):
        super().__init__()
        assert isinstance(view, OrderedViewABC), "Invalid type of view."
        self.view = view
        self.columns = list(columns)
    

class OrderedView(OrderedViewABC):
    """ View with ORDER """

    def __init__(self, view: OrderedViewABC, orders: Iterable[OrderedViewColumn]):
        super().__init__()
        assert isinstance(view, OrderedViewABC), "Invalid type of view."
        self.view = view
        self.orders = list(orders)

    @property
    def table_view(self) -> ViewABC:
        return self.view

    def iter_orders(self) -> Iterator[OrderedViewColumn]:
        yield from self.view.iter_orders()
        yield from self.orders
    

class LimitedView(ViewABC):
    """ View with LIMIT, OFFSET clause """

    def __init__(self, view: OrderedViewABC, limit: int):
        super().__init__()
        assert isinstance(view, OrderedViewABC), "Invalid type of view."
        self.view = view
        self._limit = limit


class OffsetView(ViewABC):
    """ View with LIMIT, OFFSET clause """

    def __init__(self, view: LimitedViewABC, offset: int):
        super().__init__()
        assert isinstance(view, LimitedViewABC), "Invalid type of view."
        self.view = view
        self._offset = offset


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


