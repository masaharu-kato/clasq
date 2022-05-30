"""
    View classes
"""
from abc import abstractproperty
from typing import TYPE_CHECKING, Iterable, Iterator, Optional, Union

from ..syntax.object_abc import NameLike, ObjectName
from ..syntax.query_data import QueryData
from ..syntax.exprs import AliasedExpr, ExprABC, ExprLike, ExprObjectABC, ExprObjectSet, FrozenExprObjectSet, FrozenOrderedExprObjectSet, NoneExpr, OrderedExprObject, OrderedExprObjectSet
from ..syntax.keywords import JoinType, JoinLike, OrderType
from ..syntax.values import ValueType
from ..syntax.errors import ObjectArgTypeError, ObjectArgValueError, ObjectError, ObjectNameAlreadyExistsError, ObjectNotFoundError, ObjectNotSetError
from ..utils.tabledata import RowData, TableData
from .column import FrozenOrderedNamedViewColumnSet, NamedViewColumn
from .view_abc import CustomViewABC, ViewABC, BaseViewABC, NamedViewABC, ViewWithColumnsABC, ColumnArgTypes, OrderedColumnArgTypes
from .sqltypes import AnySQLType

if TYPE_CHECKING:
    from .database import Database
 

class ViewFinal(ViewABC):

    def __init__(self) -> None:
        super().__init__()
        self._select_query: Optional[QueryData] = None
        self._result: Optional[TableData] = None

    def refresh_select_query(self) -> None:
        """ Refresh QueryData """
        assert self.selected_exprs
        # print('self.base_view.select_from_query=', self.base_view.select_from_query)
        # assert self.base_view.select_from_query
        self.base_view.refresh_select_from_query()
        self._select_query = QueryData(
            b'SELECT',
            [c.select_column_query for c in self.selected_exprs],
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

    def join(self, join_type: JoinLike, view: 'ViewABC', expr: ExprABC) -> 'JoinedView':
        return JoinedView(self, JoinType.make(join_type), view, expr)
        
    def with_args(self, *argvals, **kwargvals) -> 'ViewWithArgs':
        return ViewWithArgs(self, *argvals, **kwargvals)

    def _new_view(self, *args, **kwargs) -> 'ViewABC':
        return CustomView(*args, **kwargs)
        

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
        """ Get a parent Database object
            If not exists, returns None.

            [Abstract property]
        """
        return self.target_view.database_or_none

    @property
    def selected_exprs(self) -> FrozenOrderedExprObjectSet:
        """ Set of selected column (or expression) in this view

        Returns:
            FrozenOrderedExprObjectSet: Frozen ordered set of selected column
        """
        return self.target_view.selected_exprs

    @property
    def base_column_set(self) -> FrozenOrderedNamedViewColumnSet:
        """ Set of base column (column in named view) in the base view of this view

        Returns:
            FrozenNamedViewColumnSet: Frozen set of NamedViewColumnABC
        """
        return self.target_view.base_column_set

    @property
    def where_expr(self) -> ExprABC:
        return self.target_view.where_expr

    @property
    def orders(self) -> FrozenOrderedExprObjectSet:
        return self.target_view.orders

    @property
    def outer_orders(self) -> FrozenOrderedExprObjectSet:
        return self.target_view.outer_orders

    @property
    def groups(self) -> FrozenOrderedExprObjectSet:
        return self.target_view.groups

    @property
    def limit_value(self) -> Optional[ExprLike]:
        return self.target_view.limit_value
    
    @property
    def offset_value(self) -> Optional[ExprLike]:
        return self.target_view.offset_value

    # def refresh_select_query(self) -> None:
    #     return self.target_view.refresh_select_query()

    # @property
    # def select_query_or_none(self) -> Optional[QueryData]:
    #     return self.target_view.select_query_or_none

    # def refresh_result(self) -> None:
    #     return self.target_view.refresh_result()

    # @property
    # def result_or_none(self) -> Optional[TableData]:
    #     return self.target_view.result_or_none


class ViewWithTarget(ViewWithTargetABC):
    """ View with the other view object as a target """
    
    def __init__(self, target_view) -> None:
        super().__init__()
        self._target_view = target_view
        
    @property
    def target_view(self) -> ViewABC:
        return self._target_view


class JoinedView(ViewWithColumnsABC, ViewWithTargetABC, ViewFinal):
    """ Table View """
    def __init__(self, dest_view: ViewABC, join_type: JoinLike, join_view: ViewABC, expr: ExprABC):

        # Check if a subquery is required for view to join
        if join_view.limit_value or join_view.offset_value:
            raise ObjectArgValueError('Cannot join a `join_view` because it needs subquery.'
                'Make a SubqueryView from `join_view` and specify it.', join_view)

        self._target_view = dest_view
        self._join_type = JoinType.make(join_type)
        self._view_to_join = join_view
        self._expr_for_join = expr

        if duplicate_columns := dest_view.base_column_set & join_view.base_column_set:
            raise ObjectError('Duplicate column names:', duplicate_columns)

        super().__init__(dest_view.base_column_set | join_view.base_column_set)

        self._selected_exprs = self._merge_selected_exprs(
            dest_view.selected_exprs, join_view.selected_exprs,
            alias_format=join_view.column_alias_format)

        self._select_from_query: Optional[QueryData] = None


    def _merge_selected_exprs(self,
        exprs1: FrozenOrderedExprObjectSet,
        exprs2: FrozenOrderedExprObjectSet,
        alias_format: ObjectName,
    ) -> FrozenOrderedExprObjectSet:
        _selected_exprs = OrderedExprObjectSet(exprs1)
        for expr in exprs2:
            if expr in _selected_exprs:
                continue
            if expr.name in _selected_exprs:
                new_name = alias_format % expr.name
                _selected_exprs.add(AliasedExpr(expr, new_name))
            else:
                _selected_exprs.add(expr)
        return FrozenOrderedExprObjectSet(_selected_exprs)

    
    @property
    def target_view(self) -> 'ViewABC':
        """ Get a destination View """
        return self._target_view

    @property
    def selected_exprs(self) -> FrozenOrderedExprObjectSet:
        return self._selected_exprs

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
    def orders(self) -> FrozenOrderedExprObjectSet:
        return self.target_view.orders & self.view_to_join.outer_orders

    @property
    def outer_orders(self) -> FrozenOrderedExprObjectSet:
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


class SubqueryView(NamedViewABC):
    """ Subquery View """
    def __init__(self, target_view: ViewABC) -> None:
        self._target_view = target_view
        super().__init__(FrozenOrderedNamedViewColumnSet(
            NamedViewColumn(self, col.name, AnySQLType) # TODO: Fix type
            for col in target_view.selected_exprs))

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


class ViewWithArgs(ViewWithTarget, ViewFinal):
    """ Table View with arguments """
    def __init__(self, target_view: ViewABC, *argvals: ValueType, **kwargvals: ValueType):
        super().__init__(target_view)
        self._argvals = tuple(argvals)
        self._kwargvals = tuple(kwargvals.items())

    @property
    def selected_exprs(self):
        return self.target_view.selected_exprs

    @property
    def argvals(self):
        return self._argvals

    @property
    def kwargvals(self):
        return self._kwargvals

    def refresh_select_query(self) -> None:
        target_qd = self.target_view.select_query
        self._select_query = target_qd.call(*self.argvals, **dict(self.kwargvals))

    def __repr__(self) -> str:
        return ('VA(%s, %s, %s)'
            % (self.target_view, self.argvals, self.kwargvals))


class CustomView(CustomViewABC, ViewFinal):
    """ Table View """
    def __init__(self,
        base_view: BaseViewABC,
        *column_likes: ColumnArgTypes,
        where    : ExprABC = NoneExpr,
        groups   : Iterable[Union[NameLike, ExprObjectABC]] = (),
        orders   : Iterable[OrderedColumnArgTypes] = (),
        limit    : Optional[ExprLike] = None,
        offset   : Optional[ExprLike] = None,
        outer_orders   : Optional[Iterable[OrderedExprObject]] = None,
        **options,
    ):
        super().__init__(base_view)

        assert isinstance(base_view, BaseViewABC)
        assert isinstance(where, ExprABC)
        assert all(isinstance(c, (bytes, str, ObjectName, ExprObjectABC)) for c in groups)
        assert all(isinstance(c, (bytes, str, ObjectName, ExprObjectABC, tuple)) for c in orders)
        assert outer_orders is None or all(isinstance(c, OrderedExprObject) for c in outer_orders)

        self._selected_exprs = self._process_select_column_args(*column_likes)
        if not self._selected_exprs:
            raise ObjectArgValueError('Columns cannot be empty.')
            
        self._where = where
        self._groups = FrozenOrderedExprObjectSet(self.to_column(c) for c in groups)
        self._orders = (
            FrozenOrderedExprObjectSet(self._process_order_args(*orders)) | base_view.outer_orders)

        self._limit  = limit
        self._offset = offset

        self._outer_orders = self._orders if outer_orders is None else FrozenOrderedExprObjectSet(outer_orders)
        self._options = options

    def _process_select_column_args(self, *column_likes: ColumnArgTypes) -> FrozenOrderedExprObjectSet:

        _selected_exprs = OrderedExprObjectSet()
        
        for column_like in column_likes:

            # FrozenOrderedExprObjectSet
            if isinstance(column_like, (ExprObjectSet, FrozenExprObjectSet)):
                if _dups := _selected_exprs & column_like:
                    raise ObjectNameAlreadyExistsError(
                        'Duplicate column names:', [c.name for c in _dups])
                _selected_exprs |= column_like
                continue

            # ExprObjectABC: Append the object itself
            if isinstance(column_like, ExprObjectABC):
                new_expr = column_like

            # Tuple of ((column name | column), alias name):
            #   Make a AliasedExpr object
            elif (isinstance(column_like, tuple) and len(column_like) == 2):
                expr, name = column_like
                if (column := self.base_view.to_column_or_none(expr)) is None:
                    raise ObjectNotFoundError('Column not found.', expr)
                new_expr = AliasedExpr(column, name)
            
            # ColumnABC or NameLike:
            #   Get a column/expr object from the base view
            elif isinstance(column_like, (bytes, str, ObjectName)):
                if (column := self.base_view.column_or_none(column_like)) is None:
                    raise ObjectNotFoundError('Column not found.', column_like)
                new_expr = column

            else:
                raise ObjectArgTypeError('Invalid column type.', column_like)

            # Register the ViewColumn object to this view.
            #   If there is a same name one, raise exception.
            if new_expr.name in _selected_exprs:
                raise ObjectNameAlreadyExistsError('Name already exists.', new_expr.name)
            _selected_exprs.add(new_expr)

        return FrozenOrderedExprObjectSet(_selected_exprs)


    def _process_order_args(self, *column_orders: OrderedColumnArgTypes) -> Iterator[OrderedExprObject]:
        
        for col_order in column_orders:
            if isinstance(col_order, tuple) and len(col_order) == 2:
                expr, otype = col_order
                yield self.base_view.to_column(expr).ordered(otype)
            
            elif isinstance(col_order, (bytes, str, ObjectName)):
                _name = ObjectName(col_order)
                _otype = (
                    OrderType.ASC if _name.raw_name[0:1] == b'+' else 
                    OrderType.DESC if _name.raw_name[0:1] == b'-' else None)
                if _otype is not None:
                    _name_entity = _name.raw_name[1:]
                    yield self.base_view.column(_name_entity).ordered(_otype)
                else:
                    yield self.base_view.column(_name).ordered(OrderType.ASC)
            elif isinstance(col_order, ExprObjectABC):
                if isinstance(col_order, OrderedExprObject):
                    yield col_order
                else:
                    yield col_order.ordered(OrderType.ASC)
            else:
                raise ObjectArgTypeError('Invalid type.', col_order)
        
    @property
    def selected_exprs(self) -> FrozenOrderedExprObjectSet:
        return self._selected_exprs
        
    @property
    def where_expr(self) -> ExprABC:
        """ Get a WHERE expression of this view

        Returns:
            ExprABC: WHERE expression
        """
        return self._where

    @property
    def groups(self) -> FrozenOrderedExprObjectSet:
        """ Get GROUP BY grouping columns of this view

        Returns:
            FrozenOrderedExprObjectSet: Grouping columns
        """
        return self._groups

    @property
    def orders(self) -> FrozenOrderedExprObjectSet:
        """ Get ORDER BY ordering columns of this view

        Returns:
            FrozenOrderedExprObjectSet: Ordering columns
        """
        return self._orders

    @property
    def outer_orders(self) -> FrozenOrderedExprObjectSet:
        """ Get ORDER BY ordering columns for outer views

        Returns:
            FrozenOrderedExprObjectSet: Ordering columns
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


class CustomSingleView(CustomView):
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
        return CustomSingleView(*args, **kwargs)

    def __repr__(self) -> str:
        return 'SV(%s)' % repr(self.base_view)
