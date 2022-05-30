"""
    View abstract class
"""
from abc import ABC, abstractmethod, abstractproperty
import itertools
from typing import TYPE_CHECKING, Iterable, Optional, Tuple, Union, overload

from ..syntax.object_abc import NameLike, ObjectABC, ObjectName
from ..syntax.query_data import QueryData
from ..syntax.exprs import AliasedExpr, ExprABC, ExprLike, ExprObjectABC, ExprObjectSet, FrozenExprObjectSet, FrozenOrderedExprObjectSet, NoneExpr, OP
from ..syntax.keywords import JoinType, JoinLike, OrderTypeLike
from ..syntax.errors import NotaSelfObjectError, ObjectArgTypeError, ObjectNotFoundError, ObjectNotSetError
from ..utils.tabledata import RowData, TableData
from .column_abc import ColumnABC
from .column import FrozenOrderedNamedViewColumnSet, NamedViewColumn, NamedViewColumnABC

if TYPE_CHECKING:
    from .database import Database
    from ..connection import ConnectionABC
    from .view import JoinedView, ViewWithArgs

ColumnArgTypes = Union[ExprObjectSet, FrozenExprObjectSet, ExprObjectABC, NameLike, Tuple[Union[ColumnABC, NameLike], NameLike]]
OrderedColumnArgTypes = Union[NameLike, ExprObjectABC, Tuple[Union[NameLike, ExprObjectABC], OrderTypeLike]]

class ViewABC(ABC):
    """ View Expr """

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

    @abstractproperty
    def base_view(self) -> 'BaseViewABC':
        """ Get a base view object
            (If self is instance of BaseViewABC, returns self)
        """
        
    @property
    def base_named_view(self) -> 'NamedViewABC':
        """ Get a view with name (if not exists, get from base view recursively) """
        return self.base_view.base_named_view

    @property
    def base_name(self) -> ObjectName:
        """ Get a view name (if not exists, get from base view recursively) """
        return self.base_named_view.name

    @property
    def column_alias_format(self) -> ObjectName:
        return self.base_name + '_%s'

    @abstractproperty
    def base_column_set(self) -> FrozenOrderedNamedViewColumnSet:
        """ Set of base column (column in named view) in the base view of this view

        Returns:
            FrozenNamedViewColumnSet: Frozen set of NamedViewColumnABC
        """
        
    @abstractproperty
    def selected_exprs(self) -> FrozenOrderedExprObjectSet:
        """ Set of selected column (or expression) in this view

        Returns:
            FrozenOrderedExprObjectSet: Frozen ordered set of selected column
        """

    @abstractmethod
    def _new_view(self, *args, **kwargs) -> 'ViewABC':
        """ Create a new view """

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
        return self.database.con

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
    def groups(self) -> FrozenOrderedExprObjectSet:
        """ Get GROUP BY grouping columns of this view

        Returns:
            FrozenOrderedExprObjectSet: Grouping columns
        """

    @abstractproperty
    def orders(self) -> FrozenOrderedExprObjectSet:
        """ Get ORDER BY ordering columns of this view

        Returns:
            FrozenOrderedExprObjectSet: Ordering columns
        """

    @abstractproperty
    def outer_orders(self) -> FrozenOrderedExprObjectSet:
        """ Get ORDER BY ordering columns for outer views

        Returns:
            FrozenOrderedExprObjectSet: Ordering columns
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

    def selected_column(self, val: NameLike) -> ExprObjectABC:
        """ Get a column by its name from the selected columns

        Args:
            val (str | bytes | ObjectName): Column name

        Raises:
            ObjectNotFoundError: Column not found.

        Returns:
            ViewColumn: Column object with the specified name
        """
        name = ObjectName(val)
        if name not in self.selected_exprs:
            raise ObjectNotFoundError('Column not found.', name)
        return self.selected_exprs[name]


    def selected_column_or_none(self, val: NameLike) -> Optional[ExprObjectABC]:
        try:
            return self.selected_column(val)
        except (ObjectNotFoundError, NotaSelfObjectError):
            pass
        return None


    def column(self, val: NameLike) -> ExprObjectABC:
        """ Get a column by its name from the base columns

        Args:
            val (str | bytes | ObjectName): Column name

        Raises:
            ObjectNotFoundError: Column not found.

        Returns:
            ViewColumn: Column object with the specified name
        """
        name = ObjectName(val)
        if name in self.selected_exprs:
            return self.selected_exprs[name]
        if name in self.base_column_set:
            return self.base_column_set[name]
        raise ObjectNotFoundError('Column not found.', name)


    def col(self, val: NameLike) -> ExprObjectABC:
        """ Synonym of `column` method """
        return self.column(val)


    def column_or_none(self, val: NameLike) -> Optional[ExprObjectABC]:
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


    def get(self, val: NameLike) -> Optional[ExprObjectABC]:
        """ Synonym of `column_or_none` method """
        return self.column_or_none(val)


    def to_selected_column(self, val: Union[NameLike, ExprABC]) -> ExprObjectABC:
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

        # If a val is NameLike (bytes | str | ObjectName),
        #   get from self selected expression set
        if not isinstance(val, ExprABC):
            return self.selected_column(val)

        # If a val is ExprObjectABC,
        #   get from self selected expression set using the object name
        if isinstance(val, ExprObjectABC):
            if val in self.selected_exprs:
                return val
                
        # If a val is not ExprObjectABC and is ExprABC,
        #   search from the exprs in self selected expression set
        else:
            for sel_expr in self.selected_exprs:
                if isinstance(sel_expr, AliasedExpr) and val is sel_expr.expr:
                    return sel_expr

        raise ObjectNotFoundError(
            'The specified column or Expression is not included in this view.', val)


    def to_selected_column_or_none(self, val: Union[NameLike, ExprABC]) -> Optional[ExprObjectABC]:
        try:
            return self.to_selected_column(val)
        except (ObjectNotFoundError, NotaSelfObjectError):
            pass
        return None


    def to_column(self, val: Union[NameLike, ExprABC]) -> ExprObjectABC:
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

        # If a val is NameLike (bytes | str | ObjectName),
        #   get from self selected expression set
        if not isinstance(val, ExprABC):
            return self.column(val)

        # If a val is ExprObjectABC,
        #   get from self selected expression set using the object name
        if isinstance(val, ExprObjectABC):
            if (val in self.selected_exprs or 
                (isinstance(val, NamedViewColumnABC) and val in self.base_column_set)):
                return val
                
        # If a val is not ExprObjectABC and is ExprABC,
        #   search from the exprs in self selected expression set
        else:
            for sel_expr in self.selected_exprs:
                if isinstance(sel_expr, AliasedExpr) and val is sel_expr.expr:
                    return sel_expr

        raise ObjectNotFoundError(
            'The specified column or Expression is not included in this view.', val)

    def to_column_or_none(self, val: Union[NameLike, ExprABC]) -> Optional[ExprObjectABC]:
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

    def clone(self,
        *,
        column_likes: Optional[Iterable[ColumnArgTypes]] = None,
        where : ExprABC = NoneExpr,
        groups: Iterable[Union[NameLike, ExprObjectABC]] = (),
        orders: Iterable[OrderedColumnArgTypes] = (),
        limit : Optional[ExprLike] = None,
        offset: Optional[ExprLike] = None,
    ) -> 'ViewABC':
        # print('Clone to a new view ...')
        assert self.selected_exprs
        return self._new_view(
            self.base_view,  # TODO: ?
            *(column_likes if column_likes is not None else [self.selected_exprs]),
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
            [self.selected_exprs],
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
        return self.clone(groups=itertools.chain(
            columns, *(c for c, v in cols.items() if v)))

    def order_by(self,
        *columns: Union[NameLike, ExprObjectABC],
        **col_orders: Optional[OrderTypeLike],
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
        return self.clone(orders=itertools.chain(
            columns, ((c, v) for c, v in col_orders.items() if v is not None)))

    def limit(self, limit: ExprABC) -> 'ViewABC':
        """ Make a View object with LIMIT OFFSET clause """
        return self.clone(limit=limit)

    def offset(self, offset: ExprABC) -> 'ViewABC':
        """ Make a View object with LIMIT OFFSET clause """
        return self.clone(offset=offset)

    # def single(self) -> 'SingleView':
    #     """ Make a View object with a single result """
    #     return SingleView(self)

    @abstractmethod
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
    def __getitem__(self, val: NameLike) -> NamedViewColumn: ...

    @overload
    def __getitem__(self, val: Tuple[NameLike, ...]) -> Tuple[NamedViewColumn, ...]: ...
        
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

    @abstractmethod
    def with_args(self, *argvals, **kwargvals) -> 'ViewWithArgs':
        """ Get a view with arguments """

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
            and self.selected_exprs == view.selected_exprs
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

    def _proc_col_args(self, *collikes: ColumnArgTypes, **as_exprs: Union[NameLike, ExprABC]) -> Iterable[ColumnArgTypes]:
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
            self.refresh_select_from_query()
        assert self.select_from_query_or_none is not None
        return self.select_from_query_or_none

    @property
    def selected_exprs(self) -> FrozenOrderedExprObjectSet:
        """ Set of selected column (or expression) in this view

        Returns:
            FrozenOrderedExprObjectSet: Frozen ordered set of selected column
        """
        return FrozenOrderedExprObjectSet(self.base_column_set)
        
    @property
    def where_expr(self) -> ExprABC:
        return NoneExpr

    @property
    def orders(self) -> FrozenOrderedExprObjectSet:
        return FrozenOrderedExprObjectSet()

    @property
    def outer_orders(self) -> FrozenOrderedExprObjectSet:
        return FrozenOrderedExprObjectSet()

    @property
    def groups(self) -> FrozenOrderedExprObjectSet:
        return FrozenOrderedExprObjectSet()

    @property
    def limit_value(self) -> Optional[ExprLike]:
        return None
    
    @property
    def offset_value(self) -> Optional[ExprLike]:
        return None

    def __repr__(self) -> str:
        return 'BV(%s)' % repr(self.base_view)


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
    def base_column_set(self) -> FrozenOrderedNamedViewColumnSet:
        """ Returns a dictionary from name to View Column object

        Returns:
            FrozenNamedViewColumnSet: Frozen set of NamedViewColumn
        """
        return self.base_view.base_column_set

    @property
    def database_or_none(self) -> Optional['Database']:
        return self.base_view.database_or_none


class ViewWithColumnsABC(BaseViewABC):
    """ Unnamed table-like View ABC """
    
    def __init__(self, column_set: FrozenOrderedNamedViewColumnSet):
        super().__init__()
        assert isinstance(column_set, FrozenOrderedNamedViewColumnSet)
        assert all(isinstance(col, NamedViewColumnABC) for col in column_set)
        self._named_view_columns = column_set

    @property
    def base_column_set(self) -> FrozenOrderedNamedViewColumnSet:
        """ Returns a dictionary from name to View Column object

        Returns:
            FrozenNamedViewColumnSet: Frozen set of NamedViewColumn
        """
        return self._named_view_columns


class NamedViewABC(ViewWithColumnsABC, ObjectABC):
    """ Named View abstract class """
        
    @property
    def base_named_view(self) -> 'NamedViewABC':
        """ Get a view with name
            (Override from `ViewABC`)
        """
        return self
