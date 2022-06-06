"""
    View abstract class
"""
from __future__ import annotations
from abc import ABC, abstractmethod, abstractproperty
import itertools
from typing import TYPE_CHECKING, Iterable, TypeAlias, overload

from ...syntax.abc.exprs import ExprLike
from ...syntax.abc.keywords import JoinType, JoinLike, OrderTypeLike
from ...syntax.abc.object import NameLike, ObjectABC, ObjectName
from ...syntax.query import QueryData
from ...syntax.exprs import AliasedExpr, ExprABC, ExprObjectABC, ExprObjectSet, FrozenExprObjectSet, FrozenOrderedExprObjectSet, NoneExpr, OPs
from ...errors import NotaSelfObjectError, ObjectArgTypeError, ObjectNotFoundError, ObjectNotSetError
from ...utils.tabledata import TableData
from ..column import FrozenOrderedNamedViewColumnSet, NamedViewColumn, NamedViewColumnABC
from .column import ColumnABC

if TYPE_CHECKING:
    from .database import DatabaseABC
    from ...connection import ConnectionABC
    from ..view import JoinedView, ViewWithArgs

_ColumnWithAlias = tuple[ColumnABC | NameLike, NameLike]
ColumnArgTypes: TypeAlias = ExprObjectSet | FrozenExprObjectSet | ExprObjectABC | NameLike | _ColumnWithAlias
_OrderedColumnWithAlias = tuple[NameLike | ExprObjectABC, OrderTypeLike]
OrderedColumnArgTypes: TypeAlias = NameLike | ExprObjectABC | _OrderedColumnWithAlias

class ViewABC(ABC):
    """ View Expr """

    @abstractproperty
    def _view_name_or_none(self) -> ObjectName | None:
        """ Get a name of this view if set
            (Abstract property)

        Returns:
            ExprLike | None: Name of this view
                If not set, returns `None`.
        """
        raise NotImplementedError()

    @property
    def _view_name(self) -> ObjectName:
        if (name := self._view_name_or_none) is None:
            raise ObjectNotSetError('This view does not have a name.')
        return name

    @abstractproperty
    def _base_view(self) -> BaseViewABC:
        """ Get a base view object
            (If self is instance of BaseViewABC, returns self)
        """
        raise NotImplementedError()
        
    @property
    def _base_named_view(self) -> NamedViewABC:
        """ Get a view with name (if not exists, get from base view recursively) """
        return self._base_view._base_named_view

    @property
    def _base_name(self) -> ObjectName:
        """ Get a view name (if not exists, get from base view recursively) """
        return self._base_named_view._view_name

    @property
    def _column_alias_format(self) -> ObjectName:
        return self._base_name + '_%s'

    @abstractproperty
    def _base_column_set(self) -> FrozenOrderedNamedViewColumnSet:
        """ Set of base column (column in named view) in the base view of this view

        Returns:
            FrozenNamedViewColumnSet: Frozen set of NamedViewColumnABC
        """
        raise NotImplementedError()
        
    @abstractproperty
    def _selected_exprs(self) -> FrozenOrderedExprObjectSet:
        """ Set of selected column (or expression) in this view

        Returns:
            FrozenOrderedExprObjectSet: Frozen ordered set of selected column
        """
        raise NotImplementedError()

    @abstractmethod
    def _new_view(self, *args, **kwargs) -> ViewABC:
        """ Create a new view """
        raise NotImplementedError()

    @abstractproperty
    def _database_or_none(self) -> DatabaseABC | None:
        """ Get a parent Database object
            If not exists, returns None.

            [Abstract property]
        """
        raise NotImplementedError()

    @property
    def _database(self) -> DatabaseABC:
        """ Get a parent Database object of this view

            Raises:
                ObjectNotSetError: The database is not set.
        """
        if self._database_or_none is None:
            raise ObjectNotSetError('Database is not set.')
        return self._database_or_none

    @property
    def db(self) -> DatabaseABC:
        """ Get a parent Database object of this view
            Synonym of `database` property.

            Raises:
                ObjectNotSetError: The database is not set.

        """
        return self._database

    @property
    def _con(self) -> ConnectionABC:
        """ Get a database connection from a parent Database object

            Raises:
                ObjectNotSetError: The database is not set.
        """
        return self._database._con

    @property
    def exists_on_db(self) -> bool:
        return False # Default Implementation

    @abstractproperty
    def _where_expr(self) -> ExprABC:
        """ Get a WHERE expression of this view

        Returns:
            ExprABC: WHERE expression
        """

    @abstractproperty
    def _groups(self) -> FrozenOrderedExprObjectSet:
        """ Get GROUP BY grouping columns of this view

        Returns:
            FrozenOrderedExprObjectSet: Grouping columns
        """

    @abstractproperty
    def _orders(self) -> FrozenOrderedExprObjectSet:
        """ Get ORDER BY ordering columns of this view

        Returns:
            FrozenOrderedExprObjectSet: Ordering columns
        """

    @abstractproperty
    def _outer_orders(self) -> FrozenOrderedExprObjectSet:
        """ Get ORDER BY ordering columns for outer views

        Returns:
            FrozenOrderedExprObjectSet: Ordering columns
        """

    @abstractproperty
    def _limit_value(self) -> ExprLike | None:
        """ Get a LIMIT value of this view

        Returns:
            ExprLike | None: LIMIT value
                If not set, returns `None`.
        """

    @abstractproperty
    def _offset_value(self) -> ExprLike | None:
        """ Get a OFFSET value of this view

        Returns:
            ExprLike | None: OFFSET value
                If not set, returns `None`.
        """

    def get_selected_column(self, val: NameLike) -> ExprObjectABC:
        """ Get a column by its name from the selected columns

        Args:
            val (str | bytes | ObjectName): Column name

        Raises:
            ObjectNotFoundError: Column not found.

        Returns:
            ViewColumn: Column object with the specified name
        """
        name = ObjectName(val)
        if name not in self._selected_exprs:
            raise ObjectNotFoundError('Column not found.', name)
        return self._selected_exprs[name]


    def get_selected_column_or_none(self, val: NameLike) -> ExprObjectABC | None:
        try:
            return self.get_selected_column(val)
        except (ObjectNotFoundError, NotaSelfObjectError):
            pass
        return None


    def get_column(self, val: NameLike) -> ExprObjectABC:
        """ Get a column by its name from the base columns

        Args:
            val (str | bytes | ObjectName): Column name

        Raises:
            ObjectNotFoundError: Column not found.

        Returns:
            ViewColumn: Column object with the specified name
        """
        name = ObjectName(val)
        if name in self._selected_exprs:
            return self._selected_exprs[name]
        if name in self._base_column_set:
            return self._base_column_set[name]
        raise ObjectNotFoundError('Column not found.', name)


    def get_column_or_none(self, val: NameLike) -> ExprObjectABC | None:
        """ Get a Column object with the specified name
            
            Returns `None` if a column object with the specified name is not found.

        Args:
            val (str | bytes | ObjectName): Column name

        Returns:
            ViewColumn | None: Column object with the specified name if exists,
                otherwise, `None`.
        """
        try:
            return self.get_column(val)
        except (ObjectNotFoundError, NotaSelfObjectError):
            pass
        return None


    def _to_selected_column(self, val: NameLike | ExprABC) -> ExprObjectABC:
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
            ViewColumn | None: Column object with the specified name if exists,
                or ViewColumn object itself if it is valid,
                or ViewCOlumn object which has a given expression.
        """

        # If a val is NameLike (bytes | str | ObjectName),
        #   get from self selected expression set
        if not isinstance(val, ExprABC):
            return self.get_selected_column(val)

        # If a val is ExprObjectABC,
        #   get from self selected expression set using the object name
        if isinstance(val, ExprObjectABC):
            if val in self._selected_exprs:
                return val
                
        # If a val is not ExprObjectABC and is ExprABC,
        #   search from the exprs in self selected expression set
        else:
            for sel_expr in self._selected_exprs:
                if isinstance(sel_expr, AliasedExpr) and val is sel_expr._expr:
                    return sel_expr

        raise ObjectNotFoundError(
            'The specified column or Expression is not included in this view.', val)


    def _to_selected_column_or_none(self, val: NameLike | ExprABC) -> ExprObjectABC | None:
        try:
            return self._to_selected_column(val)
        except (ObjectNotFoundError, NotaSelfObjectError):
            pass
        return None


    def _to_column(self, val: NameLike | ExprABC) -> ExprObjectABC:
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
            ViewColumn | None: Column object with the specified name if exists,
                or ViewColumn object itself if it is valid,
                or ViewCOlumn object which has a given expression.
        """

        # If a val is NameLike (bytes | str | ObjectName),
        #   get from self selected expression set
        if not isinstance(val, ExprABC):
            return self.get_column(val)

        # If a val is ExprObjectABC,
        #   get from self selected expression set using the object name
        if isinstance(val, ExprObjectABC):
            if (val in self._selected_exprs or 
                (isinstance(val, NamedViewColumnABC) and val in self._base_column_set)):
                return val
                
        # If a val is not ExprObjectABC and is ExprABC,
        #   search from the exprs in self selected expression set
        else:
            for sel_expr in self._selected_exprs:
                if isinstance(sel_expr, AliasedExpr) and val is sel_expr._expr:
                    return sel_expr

        raise ObjectNotFoundError(
            'The specified column or Expression is not included in this view.', val)

    def _to_column_or_none(self, val: NameLike | ExprABC) -> ExprObjectABC | None:
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
            ViewColumn | None: Column object with the specified name if exists,
                or ViewColumn object itself if it is valid,
                or ViewCOlumn object which has a given expression.
                Otherwise, `None`.
        """
        try:
            return self._to_column(val)
        except (ObjectNotFoundError, NotaSelfObjectError):
            pass
        return None

    def __contains__(self, val: NameLike | ExprABC) -> bool:
        return self._to_column_or_none(val) is not None

    def clone(self,
        *,
        column_likes: Iterable[ColumnArgTypes] | None = None,
        where : ExprABC = NoneExpr,
        groups: Iterable[NameLike | ExprObjectABC] = (),
        orders: Iterable[OrderedColumnArgTypes] = (),
        limit : ExprLike | None = None,
        offset: ExprLike | None = None,
    ) -> ViewABC:
        # print('Clone to a new view ...')
        assert self._selected_exprs
        return self._new_view(
            self._base_view,  # TODO: ?
            *(column_likes if column_likes is not None else [self._selected_exprs]),
            where = self._where_expr & where,
            groups = (*self._groups, *groups),  # TODO: Add overwrite mode
            orders = (*self._orders, *orders),  # TODO: Add overwrite mode
            limit = limit if limit is not None else self._limit_value,
            offset = offset if offset is not None else self._offset_value,
        )

    def select_column(self, *cols: NameLike | ColumnABC, **as_cols: NameLike | ExprABC) -> ViewABC:
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

    def add_column(self, *cols: NameLike | ColumnABC, **as_cols: NameLike | ExprABC) -> ViewABC:
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
            [self._selected_exprs],
            self._proc_col_args(*cols, **as_cols)))

    def where(self, *exprs: ExprABC, **coleqs: ExprABC) -> ViewABC:
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
        return self.clone(where=OPs.AND(
            *exprs,
            *(self.get_column(c) == v for c, v in coleqs.items()))
        )

    def group_by(self, *columns: NameLike | ColumnABC, **cols: bool | None) -> ViewABC:
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
        *columns: NameLike | ExprObjectABC,
        **col_orders: OrderTypeLike | None,
    ) -> ViewABC:
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

    def limit(self, limit: ExprABC) -> ViewABC:
        """ Make a View object with LIMIT OFFSET clause """
        return self.clone(limit=limit)

    def offset(self, offset: ExprABC) -> ViewABC:
        """ Make a View object with LIMIT OFFSET clause """
        return self.clone(offset=offset)

    # def single(self) -> SingleView:
    #     """ Make a View object with a single result """
    #     return SingleView(self)

    @abstractmethod
    def join(self, join_type: JoinLike, view: ViewABC, expr: ExprABC) -> JoinedView:
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
        raise NotImplementedError()

    def inner_join(self, view: ViewABC, expr: ExprABC) -> JoinedView:
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

    def left_join(self, view: ViewABC, expr: ExprABC) -> ViewABC:
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

    def right_join(self, view: ViewABC, expr: ExprABC) -> JoinedView:
        """ Make a RIGHT Joined View """
        return self.join(JoinType.RIGHT, view, expr)

    def outer_join(self, view: ViewABC, expr: ExprABC) -> JoinedView:
        """ Make a OUTER Joined View """
        return self.join(JoinType.OUTER, view, expr)

    def cross_join(self, view: ViewABC, expr: ExprABC) -> JoinedView:
        """ Make a CROSS Joined View """
        return self.join(JoinType.CROSS, view, expr)

    @overload
    def __getitem__(self, val: int | slice | ExprABC | tuple[ExprABC, ...]) -> ViewABC: ...

    @overload
    def __getitem__(self, val: NameLike) -> NamedViewColumn: ...

    @overload
    def __getitem__(self, val: tuple[NameLike, ...]) -> tuple[NamedViewColumn, ...]: ...
        
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
            return self.get_column(val)
        
        if isinstance(val, ExprABC):
            return self.where(val)

        if isinstance(val, tuple):
            if all(isinstance(v, (bytes, str, ObjectName)) for v in val):
                return (*(self.get_column(v) for v in val),)

            if all(isinstance(v, ExprABC) for v in val):
                return self.where(*val)
                
            raise ObjectArgTypeError('Invalid tuple value type.', val)

        raise ObjectArgTypeError('Invalid type.', val)

    @abstractmethod
    def with_args(self, *argvals, **kwargvals) -> ViewWithArgs:
        """ Get a view with arguments """

    def result_with_args(self, *argvals, **kwargvals) -> TableData:
        return self.with_args(*argvals, **kwargvals).result

    def __call__(self, *argvals, **kwargvals) -> TableData:
        return self.result_with_args(*argvals, **kwargvals)
    
    @abstractmethod
    def _refresh_select_query(self) -> None:
        """ Refresh a SELECT query data """

    @abstractproperty
    def _select_query_or_none(self) -> QueryData | None:
        """ Get a SELECT query data if exists """
        raise NotImplementedError()

    @property
    def _select_query(self) -> QueryData:
        if self._select_query_or_none is None:
            self._refresh_select_query()
        assert self._select_query_or_none is not None
        return self._select_query_or_none
    
    @abstractmethod
    def refresh_result(self) -> None:
        """ Refresh a result """
        raise NotImplementedError()

    @abstractproperty
    def _result_or_none(self) -> TableData | None:
        """ Get a result if exists """
        raise NotImplementedError()

    def prepare_result(self) -> None:
        if self._result_or_none is None:
            return self.refresh_result()

    @property
    def is_result_ready(self) -> bool:
        return self._result_or_none is not None

    @property
    def result(self) -> TableData:
        """ Get a result table data
            Results will be generated if not generated yet.

        Returns:
            TableData: Result table data
        """
        self.prepare_result()
        assert self._result_or_none is not None
        return self._result_or_none

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

    def __hash__(self):
        return super().__hash__()

    def _check_eq(self, view: ViewABC) -> bool:
        return (self._base_view == view._base_view
            and self._selected_exprs == view._selected_exprs
            and self._where_expr  == view._where_expr
            and self._groups == view._groups
            and self._orders == view._orders
            and self._limit_value  == view._limit_value
            and self._offset_value == view._offset_value)

    def drop(self, *, if_exists=False):
        """ Run DROP VIEW query """
        if_exists = if_exists or (not self._exists_on_db)
        return self.db.execute(
            b'DROP', b'VIEW',
            b'IF NOT EXISTS' if if_exists else (), self)

    def create(self, *, if_not_exists=False, drop_if_exists=False) -> None:
        """ Create this View on the database """
        # if drop_if_exists:
        #     self.drop(if_exists=True)
        # self.db.execute(
        #     b'CREATE', b'VIEW',
        #     b'IF NOT EXISTS' if if_not_exists else (),
        #     self, b'(', [c.q_create() for c in self.iter_named_exprs()], b')'
        # )
        # self._exists_on_db = True
        # TODO: Implementation

    def _proc_view(self, viewlike: ViewABC | NameLike) -> ViewABC:
        if isinstance(viewlike, ViewABC):
            return viewlike
        return self._database[viewlike]

    def _proc_col_args(self, *collikes: ColumnArgTypes, **as_exprs: NameLike | ExprABC) -> Iterable[ColumnArgTypes]:
        return itertools.chain(
            collikes,
            (expr.aliased(name) if isinstance(expr, ExprABC) else (expr, name)
                for name, expr in as_exprs.items())
        )

    def __repr__(self) -> str:
        if self._view_name_or_none is not None:
            return 'Vabc[%s](%s)' % (self._view_name, repr(self._base_view))
        return 'Vabc(%s)' % repr(self._base_view)


class BaseViewABC(ViewABC):
    """ Base View ABC """

    @property
    def _base_view(self) -> BaseViewABC:
        return self

    @abstractmethod
    def _refresh_select_from_query(self) -> None:
        """ Refresh a SELECT query data """

    @abstractproperty
    def _select_from_query_or_none(self) -> QueryData | None:
        """ Get a SELECT query data if exists """
        raise NotImplementedError()

    @property
    def _select_from_query(self) -> QueryData:
        if self._select_from_query_or_none is None:
            self._refresh_select_from_query()
        assert self._select_from_query_or_none is not None
        return self._select_from_query_or_none

    @property
    def _selected_exprs(self) -> FrozenOrderedExprObjectSet:
        """ Set of selected column (or expression) in this view

        Returns:
            FrozenOrderedExprObjectSet: Frozen ordered set of selected column
        """
        return FrozenOrderedExprObjectSet(self._base_column_set)
        
    @property
    def _where_expr(self) -> ExprABC:
        return NoneExpr

    @property
    def _orders(self) -> FrozenOrderedExprObjectSet:
        return FrozenOrderedExprObjectSet()

    @property
    def _outer_orders(self) -> FrozenOrderedExprObjectSet:
        return FrozenOrderedExprObjectSet()

    @property
    def _groups(self) -> FrozenOrderedExprObjectSet:
        return FrozenOrderedExprObjectSet()

    @property
    def _limit_value(self) -> ExprLike | None:
        return None
    
    @property
    def _offset_value(self) -> ExprLike | None:
        return None

    def __repr__(self) -> str:
        return 'BV(%s)' % repr(self._base_view)


class CustomViewABC(ViewABC):
    """ Derived view (View with base view object) """
    def __init__(self, base_view: BaseViewABC):
        super().__init__()
        self.__base_view = base_view
            
    @property
    def _base_view(self) -> BaseViewABC:
        """ Get a base View object
            (Override from `ViewABC`)
        """
        return self.__base_view
        
    @property
    def _base_column_set(self) -> FrozenOrderedNamedViewColumnSet:
        """ Returns a dictionary from name to View Column object

        Returns:
            FrozenNamedViewColumnSet: Frozen set of NamedViewColumn
        """
        return self._base_view._base_column_set

    @property
    def _database_or_none(self) -> DatabaseABC | None:
        return self._base_view._database_or_none


class ViewWithColumnsABC(BaseViewABC):
    """ Unnamed table-like View ABC """
    
    @abstractproperty
    def _base_column_set(self) -> FrozenOrderedNamedViewColumnSet:
        """ Returns a dictionary from name to View Column object

        Returns:
            FrozenNamedViewColumnSet: Frozen set of NamedViewColumn
        """
        raise NotImplementedError()


class NamedViewABC(ViewWithColumnsABC, ObjectABC):
    """ Named View abstract class """
        
    @property
    def _base_named_view(self) -> NamedViewABC:
        """ Get a view with name
            (Override from `ViewABC`)
        """
        return self


class ViewWithTargetABC(ViewABC):
    """ View with the other view object as a target
        (Abstract class)
    """

    @abstractproperty
    def _target_view(self) -> ViewABC:
        """ Get a target view """
        raise NotImplementedError()

    @property
    def _base_view(self) -> BaseViewABC:
        return self._target_view._base_view

    @property
    def _database_or_none(self) -> DatabaseABC | None:
        """ Get a parent Database object
            If not exists, returns None.
        """
        return self._target_view._database_or_none

    @property
    def _selected_exprs(self) -> FrozenOrderedExprObjectSet:
        """ Set of selected column (or expression) in this view

        Returns:
            FrozenOrderedExprObjectSet: Frozen ordered set of selected column
        """
        return self._target_view._selected_exprs

    @property
    def _base_column_set(self) -> FrozenOrderedNamedViewColumnSet:
        """ Set of base column (column in named view) in the base view of this view

        Returns:
            FrozenNamedViewColumnSet: Frozen set of NamedViewColumnABC
        """
        return self._target_view._base_column_set

    @property
    def _where_expr(self) -> ExprABC:
        return self._target_view._where_expr

    @property
    def _orders(self) -> FrozenOrderedExprObjectSet:
        return self._target_view._orders

    @property
    def _outer_orders(self) -> FrozenOrderedExprObjectSet:
        return self._target_view._outer_orders

    @property
    def _groups(self) -> FrozenOrderedExprObjectSet:
        return self._target_view._groups

    @property
    def _limit_value(self) -> ExprLike | None:
        return self._target_view._limit_value
    
    @property
    def _offset_value(self) -> ExprLike | None:
        return self._target_view._offset_value


class ViewReferenceABC(ViewWithTargetABC):

    @property
    def _view_name_or_none(self) -> ObjectName | None:
        return self._target_view._view_name_or_none
        
    def join(self, join_type: JoinLike, view: ViewABC, expr: ExprABC) -> JoinedView:
        return self._target_view.join(join_type, view, expr)

    def _new_view(self, *args, **kwargs) -> ViewABC:
        return self._target_view._new_view(*args, **kwargs)

    def _refresh_select_query(self) -> None:
        return self._target_view._refresh_select_query()

    @property
    def _select_query_or_none(self) -> QueryData | None:
        return self._target_view._select_query_or_none

    def refresh_result(self) -> None:
        return self._target_view.refresh_result()

    @property
    def _result_or_none(self) -> TableData | None:
        return self._target_view._result_or_none
