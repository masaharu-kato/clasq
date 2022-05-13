"""
    View classes
"""
from abc import ABC, abstractmethod, abstractproperty
import itertools
from typing import TYPE_CHECKING, Dict, Iterable, Mapping, Optional, Tuple, Union, overload

from ..syntax.object_abc import NameLike, ObjectABC, ObjectName, OrderedFrozenObjset
from ..syntax.query_data import QueryData
from ..syntax.exprs import AliasedExpr, ExprABC, ExprLike, ExprObjectABC, NoneExpr, OP, OrderedExprObject
from ..syntax.keywords import JoinType, JoinLike, OrderLike, OrderType
from ..syntax.values import ValueType
from ..syntax.errors import NotaSelfObjectError, ObjectAlreadySetError, ObjectArgTypeError, ObjectArgValueError, ObjectNameAlreadyExistsError, ObjectNotFoundError, ObjectNotSetError, ObjectNotSpecifiedError
from ..utils.tabledata import RowData, TableData
from .column import ColumnABC, ViewColumn

if TYPE_CHECKING:
    from .database import Database
    from ..connection import ConnectionABC

ColumnLike = Union[ColumnABC, NameLike, AliasedExpr, Tuple[Union[ColumnABC, NameLike], NameLike]]

class ViewABC(ABC):
    """ View Expr """

    @abstractproperty
    def base_view(self) -> 'BaseViewABC':
        """ Get a base view object
            (If self is instance of BaseViewABC, returns self)
        """

    def _new_view(self, *args, **kwargs) -> 'ViewABC':
        return View(*args, **kwargs)  # Default Implementation

    @abstractproperty
    def database_or_none(self) -> Optional['Database']:
        """ Get a parent Database object
            If not exists, returns None.

            [Abstract property]
        """

    @property
    def database(self) -> 'Database':
        """ Get a parent Database object of this view

            Raises:
                ObjectNotSetError: The database is not set.
        """
        if self.database_or_none is None:
            raise ObjectNotSetError('Database is not set.')
        return self.database_or_none

    @property
    def db(self) -> 'Database':
        """ Get a parent Database object of this view
            Synonym of `database` property.

            Raises:
                ObjectNotSetError: The database is not set.

        """
        return self.database

    @property
    def cnx(self) -> 'ConnectionABC':
        """ Get a database connection from a parent Database object

            Raises:
                ObjectNotSetError: The database is not set.
        """
        return self.database.cnx

    @property
    def exists_on_db(self) -> bool:
        return False # Default Implementation

    @abstractproperty
    def where_expr(self) -> ExprABC:
        """ Get a WHERE expression of this view

        Returns:
            ExprABC: WHERE expression
        """

    @abstractproperty
    def groups(self) -> OrderedFrozenObjset[ColumnABC]:
        """ Get GROUP BY grouping columns of this view

        Returns:
            OrderedFrozenObjset[ColumnABC]: Grouping columns
        """

    @abstractproperty
    def orders(self) -> OrderedFrozenObjset[OrderedExprObject]:
        """ Get ORDER BY ordering columns of this view

        Returns:
            OrderedFrozenObjset[OrderedExprObject]: Ordering columns
        """

    @abstractproperty
    def outer_orders(self) -> OrderedFrozenObjset[OrderedExprObject]:
        """ Get ORDER BY ordering columns for outer views

        Returns:
            OrderedFrozenObjset[OrderedExprObject]: Ordering columns
        """

    @abstractproperty
    def limit_value(self) -> Optional[ExprLike]:
        """ Get a LIMIT value of this view

        Returns:
            Optional[ExprLike]: LIMIT value
                If not set, returns `None`.
        """

    @abstractproperty
    def offset_value(self) -> Optional[ExprLike]:
        """ Get a OFFSET value of this view

        Returns:
            Optional[ExprLike]: OFFSET value
                If not set, returns `None`.
        """

    @property
    def name_or_none(self) -> Optional[ObjectName]:
        """ Get a name of this view if set

        Returns:
            Optional[ExprLike]: Name of this view
                If not set, returns `None`.
        """
        return None  # Default Implementation

    @property
    def name(self) -> ObjectName:
        if self.name_or_none is None:
            raise ObjectNotSetError('This view does not have a name.')
        return self.name_or_none
        
    @property
    def base_named_view(self) -> 'ViewABC':
        """ Get a view with name (if not exists, get from base view recursively) """
        if self.name_or_none is None:
            if self.base_view is self:
                raise ObjectNotSpecifiedError('Cannot find base named view.', self)
            return self.base_view
        return self

    @property
    def base_name(self) -> ObjectName:
        """ Get a view name (if not exists, get from base view recursively) """
        return self.base_named_view.name

    @abstractproperty
    def columns_by_name(self) -> Dict[ObjectName, ViewColumn]:
        """ Returns a dictionary from name to View Column object

        Returns:
            Dict[ObjectName, ViewColumn]: Dictionary of name -> ViewColumn object
        """

    def iter_columns(self):
        return iter(self.columns_by_name.values()) 
        
    @property
    def columns(self) -> OrderedFrozenObjset[ViewColumn]:
        """ Get a set of columns in this view """
        return OrderedFrozenObjset(*self.iter_columns())

    def column(self, val: NameLike) -> ViewColumn:
        """ Get a Column object with the specified name

        Args:
            val (str | bytes | ObjectName): Column name

        Raises:
            ObjectNotFoundError: Column not found.

        Returns:
            ViewColumn: Column object with the specified name
        """
        name = ObjectName(val)
        if name not in self.columns_by_name:
            raise ObjectNotFoundError('Column not found.', name)
        return self.columns_by_name[name]

    def col(self, val: NameLike):
        """ Synonym of `column` method """
        return self.column(val)

    def column_or_none(self, val: NameLike) -> Optional[ViewColumn]:
        """ Get a Column object with the specified name
            
            Returns `None` if a column object with the specified name is not found.

        Args:
            val (str | bytes | ObjectName): Column name

        Returns:
            Optional[ViewColumn]: Column object with the specified name if exists,
                otherwise, `None`.
        """
        try:
            return self.column(val)
        except (ObjectNotFoundError, NotaSelfObjectError):
            pass
        return None

    def get(self, val: NameLike) -> Optional[ViewColumn]:
        """ Synonym of `column_or_none` method """
        return self.column_or_none(val)

    def to_column(self, val: Union[NameLike, ExprABC]) -> ViewColumn:
        """ Get a Column of a specific name, or check the Column is valid

            Get a Column object with the specified name
            or check a given Column object is valid for this View,
            or get the ViewColumn which has a given expression in this View.
            
        Raises:
            ObjectNotFoundError: Column not found.
            
        Args:
            val (str | bytes | ObjectName | ExprABC):
                Column name or column object or expression

        Returns:
            Optional[ViewColumn]: Column object with the specified name if exists,
                or ViewColumn object itself if it is valid,
                or ViewCOlumn object which has a given expression.
        """

        # val is NameLike (bytes | str | ObjectName)
        if not isinstance(val, (ColumnABC, ExprABC)):
            return self.column(val)
        
        if isinstance(val, AliasedExpr) and not isinstance(val, ViewColumn):
            view_column = self.column(val.name)
            print('AliasedExpr', repr(val), repr(view_column))
            if view_column.expr is val.expr:
                return view_column
        else:
            for view_column in self.iter_columns():
                print(repr(val), 'is', repr(view_column), '?')
                if val is view_column or val is view_column.expr:
                    return view_column

        raise ObjectNotFoundError(
            'The specified column or Expression is not included in this view.', val)

    def to_column_or_none(self, val: Union[NameLike, ExprABC]) -> Optional[ViewColumn]:
        """ Get a Column of a specific name, or check the Column is valid

            Get a Column object with the specified name
            or check a given Column object is valid for this View,
            or get the ViewColumn which has a given expression in this View.
            
            Returns `None` if a column object with the specified name is not found
            or the given column object is not valid for this View object.

        Args:
            val (str | bytes | ObjectName | ExprABC):
                Column name or column object or expression

        Returns:
            Optional[ViewColumn]: Column object with the specified name if exists,
                or ViewColumn object itself if it is valid,
                or ViewCOlumn object which has a given expression.
                Otherwise, `None`.
        """
        try:
            return self.to_column(val)
        except (ObjectNotFoundError, NotaSelfObjectError):
            pass
        return None

    def __contains__(self, val: Union[NameLike, ExprABC]) -> bool:
        return self.to_column_or_none(val) is not None

    def to_column_with_order(self, val: Union[NameLike, ExprABC]) -> OrderedExprObject:
        if isinstance(val, ExprABC):    
            non_ordered_val = val.non_ordered if isinstance(val, ExprObjectABC) else val
            order_type = val.order_type if isinstance(val, ExprObjectABC) else OrderType.ASC
            return self.to_column(non_ordered_val).ordered(order_type)
        name = ObjectName(val)
        opt_order_type: Optional[OrderType] = (
            OrderType.ASC if name.raw_name[0:1] == b'+' else 
            OrderType.DESC if name.raw_name[0:1] == b'-' else None)
        if opt_order_type is not None:
            return self.column(name.raw_name[1:]).ordered(opt_order_type)
        return self.column(name).ordered(OrderType.ASC)

    def clone(self,
        *,
        column_likes: Optional[Iterable[ColumnLike]] = None,
        where : ExprABC = NoneExpr,
        groups: Iterable[ColumnABC] = (),
        orders: Iterable[OrderedExprObject] = (),
        limit : Optional[ExprLike] = None,
        offset: Optional[ExprLike] = None,
    ) -> 'ViewABC':
        # print('Clone to a new view ...')
        assert self.columns
        return self._new_view(
            base_view = self.base_view,  # TODO: ?
            column_likes = (column_likes if column_likes is not None
                            else self.iter_columns()),
            where = self.where_expr & where,
            groups = (*self.groups, *groups),  # TODO: Add overwrite mode
            orders = (*self.orders, *orders),  # TODO: Add overwrite mode
            limit = limit if limit is not None else self.limit_value,
            offset = offset if offset is not None else self.offset_value,
        )

    def select_column(self, *cols: Union[NameLike, ColumnABC], **as_cols: Union[NameLike, ExprABC]) -> 'ViewABC':
        """ Clone this view with a new set of columns

        Args:
            *cols (NameLike | ColumnABC): Column object or its name to select

            *as_cols (NameLike | ExprABC): Column name or any expression object to select
                The keyword string will be an alias of the column or expression.

            If a name specified, get the Column object from the base view.
        
        Raises:
            ObjectNotFoundError: The specified name of column was not found.

        Returns:
            ViewABC: New View object with a new set of columns
        """
        return self.clone(column_likes=self._proc_col_args(*cols, **as_cols))

    def add_column(self, *cols: Union[NameLike, ColumnABC], **as_cols: Union[NameLike, ExprABC]) -> 'ViewABC':
        """ Clone this view with additional columns

        Args:
            *cols (NameLike | ColumnABC): Column object or its name to select

            *as_cols (NameLike | ExprABC): Column name or any expression object to select
                The keyword string will be an alias of the column or expression.

            If a name specified, get the Column object from the base view.
        
        Raises:
            ObjectNotFoundError: The specified name of column was not found.

        Returns:
            ViewABC: New View object with additional columns
        """
        return self.clone(column_likes=itertools.chain(
            self.iter_columns(),
            self._proc_col_args(*cols, **as_cols)))

    def where(self, *exprs: ExprABC, **coleqs: ExprABC) -> 'ViewABC':
        """ Clone this view with additional WHERE condition(s)

        Args:
            *exprs (ExprABC): Condition expression(s).
                If multiple expressions are specified,
                there will be joined with AND (`&`).

            *coleqs (ExprABC): Equal condition with column names and value 
                The Column object will be taken from the base view,
                based on the keyword string.

        Examples:
            - column 'id' equals to 25:
                - `view.where(view['id'] == 25)`
                - `view.where(id=25)`
            
            - column 'name' equals to 'John' and column 'age' equals to 24
                - `view.where((view['name'] == 'John') & (view['age'] == 24))`
                - `view.where(view['name'] == 'John', view['age'] == 24)`
                - `view.where(name='John', age=24)`
            
            - column 'age' is 24 or more
                - `view.where(view['age'] >= 24)`

        Returns:
            ViewABC: New View object with WHERE conditions
        """
        return self.clone(where=OP.AND(
            *exprs,
            *(self.column(c) == v for c, v in coleqs.items()))
        )

    def group_by(self, *columns: Union[NameLike, ColumnABC], **cols: Optional[bool]) -> 'ViewABC':
        """ Clone this view with additional grouping columns

        Args:
            *cols (NameLike | ColumnABC): Column object or its name for grouping

            *as_cols (NameLike | ExprABC): Column names for grouping
                Specify column name on keyword, `True` on value.

            If a name specified, get the Column object from the base view.

        Examples:
            - Group by column 'A':
                - `view.group_by('A')`  
                - `view.order_by(view['A'])`  
                - `view.order_by(A=True)`  
            
            - Group by column 'A' and column 'B'
                - `view.group_by('A', 'B')`  
                - `view.group_by(view['A'], view['B'])`  
                - `view.group_by(A=True, B=True)`  

        Returns:
            ViewABC: New View object with grouping columns
        """
        return self.clone(groups=[
            *(self.to_column(c) for c in columns),
            *(self.column(c) for c, v in cols.items() if v)
        ])

    def order_by(self,
        *columns: Union[NameLike, ColumnABC, OrderedExprObject[ColumnABC]],
        **col_orders: Optional[OrderLike],
    ) -> 'ViewABC':
        """ Clone this view with additional order columns

        Args:
            *cols (NameLike | ColumnABC | OrderedExprObject[ColumnABC]):
                Column object or column name for order.

                The normal Column object will be treat as ASC order.
                Add `+` (ASC) or `-` (DESC) to the left side of column
                or column name.

            *as_cols (NameLike | ExprABC): Column names for order
                Specify column name on keyword.
                If ASC order, specify `True` or `OrderType.ASC` on value.
                If DESC order, specify `False` or `OrderType.DESC` on value.
                If None is specified on value, the column will be ignored.

            If a name specified, get the Column object from the base view.

        Examples:
            - Order by column 'A' with ASC order:
                - `view.order_by('A')`
                - `view.order_by('+A')`
                - `view.order_by(view['A'])`
                - `view.order_by(+view['A'])`
                - `view.order_by(A=True)`
            
            - Order by column 'A' with DESC order:
                - `view.order_by('-A')`
                - `view.order_by(-view['A'])`  
                - `view.order_by(A=False)`
            
            - Order by column 'A' with ASC order, column 'B' with DESC order
                - `view.order_by('A', '-B')`
                - `view.order_by('+A', '-B')`
                - `view.order_by(view['A'], -view['B'])`
                - `view.order_by(+view['A'], -view['B'])`
                - `view.order_by(A=True, B=False)`  

        Returns:
            ViewABC: New View object with grouping columns
        """
        return self.clone(orders=[
            *(self.to_column_with_order(c) for c in columns),
            *(self.column(c).ordered(v) for c, v in col_orders.items() if v is not None)
        ])

    def limit(self, limit: ExprABC) -> 'ViewABC':
        """ Make a View object with LIMIT OFFSET clause """
        return self.clone(limit=limit)

    def offset(self, offset: ExprABC) -> 'ViewABC':
        """ Make a View object with LIMIT OFFSET clause """
        return self.clone(offset=offset)

    # def single(self) -> 'SingleView':
    #     """ Make a View object with a single result """
    #     return SingleView(self)

    def join(self, join_type: JoinLike, view: 'ViewABC', expr: ExprABC) -> 'JoinedView':
        """ Make a Joined View

        Args:
            join_type (JoinLike): Join type. 
                Speficy by strings from 
                INNER, LEFT, RIGHT, OUTER, CROSS. (Case insensitive)

            view (ViewABC): Other view object to join
            expr (ExprABC): Condition for join 

        Examples:
            Inner join 'categories' view with the own column 'category_id':
                `view.join('INNER', categories, view['category_id'] == categories['id'])`

        Returns:
            JoinedView: New Joined View object
        """
        return JoinedView(self, JoinType.make(join_type), view, expr)

    def inner_join(self, view: 'ViewABC', expr: ExprABC) -> 'JoinedView':
        """ Make a INNER Joined View

        Args:
            view (ViewABC): Other view object to join
            expr (ExprABC): Condition for join 

        Examples:
            Inner join 'categories' view with the own column 'category_id':
                `view.inner_join(categories, view['category_id'] == categories['id'])`

        Returns:
            JoinedView: New Joined View object
        """
        return self.join(JoinType.INNER, view, expr)

    def left_join(self, view: 'ViewABC', expr: ExprABC) -> 'ViewABC':
        """ Make a LEFT Joined View

        Args:
            view (ViewABC): Other view object to join
            expr (ExprABC): Condition for join 

        Examples:
            Left join 'categories' view with the own column 'category_id':
                `view.left_join(categories, view['category_id'] == categories['id'])`

        Returns:
            JoinedView: New Joined View object
        """
        """ Make a LEFT Joined View """
        return self.join(JoinType.LEFT, view, expr)

    def right_join(self, view: 'ViewABC', expr: ExprABC) -> 'JoinedView':
        """ Make a RIGHT Joined View """
        return self.join(JoinType.RIGHT, view, expr)

    def outer_join(self, view: 'ViewABC', expr: ExprABC) -> 'JoinedView':
        """ Make a OUTER Joined View """
        return self.join(JoinType.OUTER, view, expr)

    def cross_join(self, view: 'ViewABC', expr: ExprABC) -> 'JoinedView':
        """ Make a CROSS Joined View """
        return self.join(JoinType.CROSS, view, expr)

    @overload
    def __getitem__(self, val: Union[int, slice, ExprABC, Tuple[ExprABC, ...]]) -> 'ViewABC': ...

    @overload
    def __getitem__(self, val: NameLike) -> ViewColumn: ...

    @overload
    def __getitem__(self, val: Tuple[NameLike, ...]) -> Tuple[ViewColumn, ...]: ...
        
    def __getitem__(self, val):

        if isinstance(val, int):
            return self.clone(offset=val, limit=1) # TODO: Implementation

        if isinstance(val, slice):
            assert not val.step # TODO: Implementation
            if val.start:
                if val.stop:
                    return self.clone(offset=val.start, limit=(val.stop-val.start))
                return self.clone(offset=val.start)
            else:
                if val.stop:
                    return self.clone(limit=val.stop)
            return self

        if isinstance(val, (bytes, str, ObjectName)):
            return self.column(val)
        
        if isinstance(val, ExprABC):
            return self.where(val)

        if isinstance(val, tuple):
            if all(isinstance(v, (bytes, str, ObjectName)) for v in val):
                return (*(self.column(v) for v in val),)

            if all(isinstance(v, ExprABC) for v in val):
                return self.where(*val)
                
            raise ObjectArgTypeError('Invalid tuple value type.', val)

        raise ObjectArgTypeError('Invalid type.', val)

    def with_args(self, *argvals, **kwargvals) -> 'ViewWithArgs':
        return ViewWithArgs(self, *argvals, **kwargvals)

    def result_with_args(self, *argvals, **kwargvals) -> TableData:
        return self.with_args(*argvals, **kwargvals).result

    def __call__(self, *argvals, **kwargvals) -> TableData:
        return self.result_with_args(*argvals, **kwargvals)
    
    @abstractmethod
    def refresh_select_query(self) -> None:
        """ Refresh a SELECT query data """

    @abstractproperty
    def select_query_or_none(self) -> Optional[QueryData]:
        """ Get a SELECT query data if exists """

    @property
    def select_query(self) -> QueryData:
        if self.select_query_or_none is None:
            self.refresh_select_query()
        assert self.select_query_or_none is not None
        return self.select_query_or_none
    
    @abstractmethod
    def refresh_result(self) -> None:
        """ Refresh a result """

    @abstractproperty
    def result_or_none(self) -> Optional[TableData]:
        """ Get a result if exists """

    def prepare_result(self) -> None:
        if self.result_or_none is None:
            return self.refresh_result()

    @property
    def is_result_ready(self) -> bool:
        return self.result_or_none is not None

    @property
    def result(self) -> TableData:
        """ Get a result table data
            Results will be generated if not generated yet.

        Returns:
            TableData: Result table data
        """
        self.prepare_result()
        assert self.result_or_none is not None
        return self.result_or_none

    def __iter__(self):
        assert False
        return iter(self.result)

    def __len__(self):
        return len(self.result)

    def __bool__(self):
        return bool(self.result)

    def __eq__(self, value):
        if isinstance(value, TableData):
            return self.result == value
        return super().__eq__(value)

    def check_eq(self, view: 'ViewABC') -> bool:
        return (self.base_view == view.base_view
            and self.columns == view.columns
            and self.where_expr  == view.where_expr
            and self.groups == view.groups
            and self.orders == view.orders
            and self.limit_value  == view.limit_value
            and self.offset_value == view.offset_value)

    def drop(self, *, if_exists=False):
        """ Run DROP VIEW query """
        if_exists = if_exists or (not self._exists_on_db)
        return self.db.execute(
            b'DROP', b'VIEW',
            b'IF NOT EXISTS' if if_exists else None, self)

    def create(self, *, if_not_exists=False, drop_if_exists=False) -> None:
        """ Create this View on the database """
        # if drop_if_exists:
        #     self.drop(if_exists=True)
        # self.db.execute(
        #     b'CREATE', b'VIEW',
        #     b'IF NOT EXISTS' if if_not_exists else None,
        #     self, b'(', [c.q_create() for c in self.iter_named_exprs()], b')'
        # )
        # self._exists_on_db = True
        # TODO: Implementation

    def _proc_view(self, viewlike: Union['ViewABC', NameLike]) -> 'ViewABC':
        if isinstance(viewlike, ViewABC):
            return viewlike
        return self.database[viewlike]

    def _proc_col_args(self, *collikes: ColumnLike, **as_exprs: Union[NameLike, ExprABC]) -> Iterable[ColumnLike]:
        return itertools.chain(
            collikes,
            (expr.aliased(name) if isinstance(expr, ExprABC) else (expr, name)
                for name, expr in as_exprs.items())
        )

    def __repr__(self) -> str:
        if self.name_or_none is not None:
            return 'Vabc[%s](%s)' % (self.name, repr(self.base_view))
        return 'Vabc(%s)' % repr(self.base_view)


class BaseViewABC(ViewABC):
    """ Base View ABC """

    @property
    def base_view(self) -> 'BaseViewABC':
        return self

    @abstractmethod
    def refresh_select_from_query(self) -> None:
        """ Refresh a SELECT query data """

    @abstractproperty
    def select_from_query_or_none(self) -> Optional[QueryData]:
        """ Get a SELECT query data if exists """
        raise NotImplementedError()

    @property
    def select_from_query(self) -> QueryData:
        if self.select_from_query_or_none is None:
            return self.refresh_select_from_query()
        assert self.select_from_query_or_none is not None
        return self.select_from_query_or_none
        
    @property
    def where_expr(self) -> ExprABC:
        return NoneExpr

    @property
    def orders(self) -> OrderedFrozenObjset[OrderedExprObject]:
        return OrderedFrozenObjset()

    @property
    def outer_orders(self) -> OrderedFrozenObjset[OrderedExprObject]:
        return OrderedFrozenObjset()

    @property
    def groups(self) -> OrderedFrozenObjset[ColumnABC]:
        return OrderedFrozenObjset()

    @property
    def limit_value(self) -> Optional[ExprLike]:
        return None
    
    @property
    def offset_value(self) -> Optional[ExprLike]:
        return None

    def __repr__(self) -> str:
        return 'BV(%s)' % repr(self.base_view)


class ViewWithResult(ViewABC):

    def __init__(self) -> None:
        super().__init__()
        self._select_query: Optional[QueryData] = None
        self._result: Optional[TableData] = None

    def refresh_select_query(self) -> None:
        """ Refresh QueryData """
        assert self.columns
        # print('self.base_view.select_from_query=', self.base_view.select_from_query)
        # assert self.base_view.select_from_query
        self.base_view.refresh_select_from_query()
        self._select_query = QueryData(
            b'SELECT',
            [c.select_column_query for c in self.columns],
            b'FROM', self.base_view.select_from_query,
            (b'WHERE', self.where_expr) if self.where_expr is not NoneExpr else None,
            (b'GROUP', b'BY', [*self.groups]) if self.groups else None,
            (b'ORDER', b'BY', [c.ordered_query for c in self.orders]) if self.orders else None,
            (b'LIMIT', self.limit_value) if self.limit_value else None,
            (b'OFFSET', self.offset_value) if self.offset_value else None,
        )

    @property
    def select_query_or_none(self) -> Optional[QueryData]:
        return self._select_query

    def refresh_result(self) -> None:
        self._result = self.db.query(self.select_query)

    @property
    def result_or_none(self) -> Optional[TableData]:
        return self._result

    def aliased(self, name: NameLike) -> 'NamedView':
        """ Get a aliased View object of this view

        Args:
            name (NameLike): Alias name

        Returns:
            NamedView: Aliased view object of this view
        """
        return NamedView(self, name)

    def as_(self, name: NameLike) -> 'NamedView':
        """ Get a aliased View object of this view
            Synonym of `aliased` method.

        Args:
            name (NameLike): Alias name

        Returns:
            NamedView: Aliased view object of this view
        """
        return self.aliased(name)
        

class CustomViewABC(ViewABC):
    """ Derived view (View with base view object) """
    def __init__(self, base_view: BaseViewABC):
        super().__init__()
        self._base_view = base_view
            
    @property
    def base_view(self) -> 'BaseViewABC':
        """ Get a base View object
            (Override from `ViewABC`)
        """
        return self._base_view

    @property
    def database_or_none(self) -> Optional['Database']:
        return self.base_view.database_or_none

    @property
    def columns_by_name(self) -> Dict[ObjectName, ViewColumn]:
        return self.base_view.columns_by_name


class NamedViewABC(ViewABC, ObjectABC):
    """ Named View ABC """


class ViewWithColumnsABC(BaseViewABC, ViewWithResult):
    """ Unnamed table-like View ABC """
    
    def __init__(self, view_columns_by_name: Dict[ObjectName, ViewColumn]):
        super().__init__()
        self._view_columns_by_name = view_columns_by_name

    @property
    def columns_by_name(self) -> Dict[ObjectName, ViewColumn]:
        return self._view_columns_by_name


class ViewWithTargetABC(ViewABC):
    """ View with the other view object as a target
        (Abstract class)
    """

    @abstractproperty
    def target_view(self) -> ViewABC:
        """ Get a target view """

    @property
    def base_view(self) -> 'BaseViewABC':
        return self.target_view.base_view

    @property
    def database_or_none(self) -> Optional['Database']:
        return self.target_view.database_or_none

    @property
    def columns_by_name(self) -> Dict[ObjectName, ViewColumn]:
        return self.target_view.columns_by_name

    @property
    def where_expr(self) -> ExprABC:
        return self.target_view.where_expr

    @property
    def orders(self) -> OrderedFrozenObjset[OrderedExprObject]:
        return self.target_view.orders

    @property
    def outer_orders(self) -> OrderedFrozenObjset[OrderedExprObject]:
        return self.target_view.outer_orders

    @property
    def groups(self) -> OrderedFrozenObjset[ColumnABC]:
        return self.target_view.groups

    @property
    def limit_value(self) -> Optional[ExprLike]:
        return self.target_view.limit_value
    
    @property
    def offset_value(self) -> Optional[ExprLike]:
        return self.target_view.offset_value

    def refresh_select_query(self) -> None:
        return self.target_view.refresh_select_query()

    @property
    def select_query_or_none(self) -> Optional[QueryData]:
        return self.target_view.select_query_or_none

    def refresh_result(self) -> None:
        return self.target_view.refresh_result()

    @property
    def result_or_none(self) -> Optional[TableData]:
        return self.target_view.result_or_none


class ViewWithTarget(ViewWithTargetABC):
    """ View with the other view object as a target """
    
    def __init__(self, target_view) -> None:
        super().__init__()
        self._target_view = target_view
        
    @property
    def target_view(self) -> ViewABC:
        return self._target_view


class JoinedView(ViewWithColumnsABC, ViewWithTargetABC):
    """ Table View """
    def __init__(self, dest_view: ViewABC, join_type: JoinLike, join_view: ViewABC, expr: ExprABC):

        # Check if a subquery is required for view to join
        if join_view.limit_value or join_view.offset_value:
            raise ObjectArgValueError('Cannot join a `join_view` because it needs subquery.'
                'Make a SubqueryView from `join_view` and specify it.', join_view)

        new_columns_by_name = {**dest_view.columns_by_name}
        for name, view_column in join_view.columns_by_name.items():
            if (new_name := b'%s.%s' % (join_view.base_name, name)) in new_columns_by_name:
                raise ObjectAlreadySetError('New column alias already exists.', new_name)
            renamed_view_column = view_column.renamed(new_name)
            new_columns_by_name[renamed_view_column.name] = renamed_view_column

        super().__init__(new_columns_by_name)

        self._target_view = dest_view
        self._join_type = JoinType.make(join_type)
        self._view_to_join = join_view
        self._expr_for_join = expr

        self._select_from_query: Optional[QueryData] = None
    
    @property
    def target_view(self) -> 'ViewABC':
        """ Get a destination View """
        return self._target_view

    @property
    def join_type(self) -> JoinType:
        return self._join_type

    @property
    def view_to_join(self) -> 'ViewABC':
        if self._view_to_join is None:
            raise ObjectNotSetError('View to join is not set.')
        return self._view_to_join

    @property
    def expr_for_join(self) -> 'ExprABC':
        if self._expr_for_join is None:
            raise ObjectNotSetError('Expr for join is not set.')
        return self._expr_for_join

    @property
    def orders(self) -> OrderedFrozenObjset[OrderedExprObject]:
        return self.target_view.orders & self.view_to_join.outer_orders

    @property
    def outer_orders(self) -> OrderedFrozenObjset[OrderedExprObject]:
        return self.orders

    def refresh_select_from_query(self) -> None:
        """ Refresh QueryData for SELECT FROM """
        on_expr = (self.expr_for_join & self.view_to_join.where_expr)
        # print('on_expr = ', on_expr)
        self.target_view.base_view.refresh_select_from_query()
        target_from_query = self.target_view.base_view.select_from_query
        assert target_from_query is not None
        self._select_from_query = QueryData(
            b'(', target_from_query, (
                self.join_type, b'JOIN',
                self.view_to_join.base_view.select_from_query,
                (b'ON', on_expr) if on_expr is not NoneExpr else None
            ), b')'
        )

    @property
    def select_from_query_or_none(self) -> Optional[QueryData]:
        return self._select_from_query

    def __repr__(self) -> str:
        return ('JoinV(%s %s %s)'
            % (self.target_view, self.join_type.name, self.view_to_join))


class NamedViewWithColumnsABC(ViewWithColumnsABC, ObjectABC):
    def __init__(self, exprs_by_name: Mapping[ObjectName, ExprABC]) -> None:
        super().__init__({
            name: ViewColumn(self, name, col)
            for name, col in exprs_by_name.items()
        })


class SubqueryView(NamedViewWithColumnsABC, ViewWithTargetABC):
    """ Subquery View """
    def __init__(self, target_view: NamedViewABC) -> None:
        self._target_view = target_view
        if not isinstance(target_view, NamedViewABC):
            raise ObjectArgTypeError('View objects for subquery must have an alias name.', target_view)
        super().__init__(target_view.columns_by_name)

    @property
    def target_view(self) -> ViewABC:
        return self._target_view

    def append_to_query_data(self, qd: 'QueryData') -> None:
        qd += self.name

    def refresh_select_from_query(self) -> None:
        return self.target_view.refresh_select_query()

    @property
    def select_from_query_or_none(self) -> Optional[QueryData]:
        return QueryData(
            b'(', self.target_view.select_query, b')',
            b'AS', self.target_view)

    def __repr__(self) -> str:
        return 'SqV(%s)' % self.target_view


class ViewWithArgs(ViewWithTarget, ViewWithResult):
    """ Table View with arguments """
    def __init__(self, target_view: ViewABC, *argvals: ValueType, **kwargvals: ValueType):
        super().__init__(target_view)
        self._argvals = tuple(argvals)
        self._kwargvals = tuple(kwargvals)

    @property
    def argvals(self):
        return self._argvals

    @property
    def kwargvals(self):
        return self._kwargvals

    def refresh_select_query(self) -> None:
        base_qd = self.base_view.select_query
        self._select_query = base_qd.call(*self.argvals, **dict(self.kwargvals))

    def __repr__(self) -> str:
        return ('VA(%s, %s, %s)'
            % (self.target_view, self.argvals, self.kwargvals))


class NamedView(NamedViewABC, ViewWithTarget):
    """ Named View """
    def __init__(self, target_view: ViewABC, name: NameLike) -> None:
        super().__init__(target_view)
        self._name = ObjectName(name)

    @property
    def name_or_none(self) -> Optional[ObjectName]:
        """ Get a view name
            (Override from `ViewABC`) """
        return self._name

    def append_to_query_data(self, qd: 'QueryData') -> None:
        qd += self.name

    def __repr__(self) -> str:
        return 'V[%s](%s)' % (self.name, self.base_view)


class View(CustomViewABC, ViewWithResult):
    """ Table View """
    def __init__(self, *,
        base_view: BaseViewABC,
        column_likes: Iterable[ColumnLike],
        where    : ExprABC = NoneExpr,
        groups   : Iterable[ColumnABC] = (),
        orders   : Iterable[OrderedExprObject] = (),
        limit    : Optional[ExprLike] = None,
        offset   : Optional[ExprLike] = None,
        outer_orders   : Optional[Iterable[OrderedExprObject]] = None,
        column_alias_format: NameLike = b'%s',
        exists_on_db: bool = False,
        **options,
    ):
        super().__init__(base_view)

        assert isinstance(base_view, BaseViewABC)
        # assert all(isinstance(cl, (ColumnABC, bytes, str, ObjectName, AliasedExpr, tuple)) for cl in column_likes)
        assert isinstance(where, ExprABC)
        assert all(isinstance(c, ColumnABC) for c in groups)
        assert all(isinstance(c, OrderedExprObject) for c in orders)
        assert outer_orders is None or all(isinstance(c, OrderedExprObject) for c in outer_orders)

        self._where = where
        self._groups = OrderedFrozenObjset(*groups)
        self._orders = OrderedFrozenObjset(*orders, *base_view.outer_orders)
        self._limit  = limit
        self._offset = offset

        self._outer_orders = self._orders if outer_orders is None else OrderedFrozenObjset(outer_orders)
        self._column_alias_format = ObjectName(column_alias_format)
        self._exists_on_db = exists_on_db
        self._options = options

        # Process each column expressions
        self._vcols_by_name: Dict[ObjectName, ViewColumn] = {}

        for column_like in column_likes:

            # Tuple of ((column name | column), alias name):
            #   Get a ViewColumn object from the base view using the given column name,
            #   and make a renamed ViewColumn with the given alias name.
            if isinstance(column_like, tuple) and len(column_like) == 2:
                column_ref, alias = column_like
                if (column := base_view.to_column_or_none(column_ref)) is None:
                    raise ObjectNotFoundError('Column not found.', column_ref)
                view_column = column.renamed(alias)
            
            # ColumnABC or NameLike:
            #   Get a ViewColumn object from the base view using the given value.
            #   If a ViewColumn is found, use that.
            #   If not found and the value is NameLike, raise exception.
            #   If not found and the value is ColumnABC,
            #   make a new ViewColumn from the column. 
            elif isinstance(column_like, (ColumnABC, bytes, str, ObjectName)):
                if (column := base_view.to_column_or_none(column_like)) is not None:
                    view_column = column
                else:
                    raise ObjectNotFoundError('Column not found.', column_like)
                    # if not isinstance(column_like, ColumnABC):
                    #     raise ObjectNotFoundError('Column not found.', column_like)
                    # view_column = ViewColumn(base_view, column_like.name, column_like)
            
            # Aliased expression:
            #   Make a ViewColumn with its alias and expression
            elif isinstance(column_like, AliasedExpr):
                view_column = ViewColumn(base_view, column_like.name, column_like.expr)

            else:
                raise ObjectArgTypeError('Invalid column type.', column_like)

            # Register the ViewColumn object to this view.
            #   If there is a same name one, raise exception.
            if view_column.name in self._vcols_by_name:
                raise ObjectNameAlreadyExistsError('Name already exists.', view_column)
            self._vcols_by_name[view_column.name] = view_column
            
        if not self._vcols_by_name:
            raise ObjectArgValueError('Columns cannot be empty.')

    @property
    def columns_by_name(self):
        return self._vcols_by_name
        
    @property
    def columns(self) -> OrderedFrozenObjset[ViewColumn]:
        return OrderedFrozenObjset(*self._vcols_by_name.values())  # Default Implementation
        
    @property
    def where_expr(self) -> ExprABC:
        """ Get a WHERE expression of this view

        Returns:
            ExprABC: WHERE expression
        """
        return self._where

    @property
    def groups(self) -> OrderedFrozenObjset[ColumnABC]:
        """ Get GROUP BY grouping columns of this view

        Returns:
            OrderedFrozenObjset[ColumnABC]: Grouping columns
        """
        return self._groups

    @property
    def orders(self) -> OrderedFrozenObjset[OrderedExprObject]:
        """ Get ORDER BY ordering columns of this view

        Returns:
            OrderedFrozenObjset[OrderedExprObject]: Ordering columns
        """
        return self._orders

    @property
    def outer_orders(self) -> OrderedFrozenObjset[OrderedExprObject]:
        """ Get ORDER BY ordering columns for outer views

        Returns:
            OrderedFrozenObjset[ColumnABC]: Ordering columns
        """
        return self._outer_orders

    @property
    def limit_value(self) -> Optional[ExprLike]:
        """ Get a LIMIT value of this view

        Returns:
            Optional[ExprLike]: LIMIT value
                If not set, returns `None`.
        """
        return self._limit

    @property
    def offset_value(self) -> Optional[ExprLike]:
        """ Get a OFFSET value of this view

        Returns:
            Optional[ExprLike]: OFFSET value
                If not set, returns `None`.
        """
        return self._offset

    def __repr__(self) -> str:
        return 'V(%s)' % repr(self.base_view)


class SingleView(View):
    """ Single result view """

    @property
    def result(self): # -> RowData:
        return super().result[0]

    def __iter__(self):
        return iter(self.result)

    def __getitem__(self, val):

        if isinstance(val, (bytes, str)):
            val = val if isinstance(val, bytes) else val.encode()
            return self.result[val]

        return super().__getitem__(val)

    def result_with_args(self, *argvals, **kwargvals): # -> RowData:
        return self.with_args(*argvals, **kwargvals).result

    def __call__(self, *argvals, **kwargvals): # -> RowData:
        return self.result_with_args(*argvals, **kwargvals)

    def _new_view(self, *args, **kwargs): # -> 'View':
        return SingleView(*args, **kwargs)

    def __repr__(self) -> str:
        return 'SV(%s)' % repr(self.base_view)
