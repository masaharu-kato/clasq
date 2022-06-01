"""
    View classes
"""
from __future__ import annotations
from typing import Iterable, Iterator

from ..schema.abc.column import NamedViewColumnABC
from ..syntax.abc.object import NameLike, ObjectName
from ..syntax.query_data import QueryData
from ..syntax.exprs import AliasedExpr, ExprABC, ExprLike, ExprObjectABC, ExprObjectSet, FrozenExprObjectSet, FrozenOrderedExprObjectSet, NoneExpr, OrderedExprObject, OrderedExprObjectSet
from ..syntax.keywords import JoinType, JoinLike, OrderType
from ..syntax.values import ValueType
from ..syntax.errors import ObjectArgTypeError, ObjectArgValueError, ObjectError, ObjectNameAlreadyExistsError, ObjectNotFoundError, ObjectNotSetError
from ..utils.tabledata import TableData
from .abc.view import CustomViewABC, NamedViewABC, ViewABC, BaseViewABC, ViewWithColumnsABC, ColumnArgTypes, OrderedColumnArgTypes, ViewWithTargetABC
from .column import FrozenOrderedNamedViewColumnSet, NamedViewColumn
from .sqltypes import AnySQLType
 

class ViewWithColumns(ViewWithColumnsABC):
    """ Unnamed table-like View ABC """
    
    def __init__(self, column_set: FrozenOrderedNamedViewColumnSet):
        super().__init__()
        assert isinstance(column_set, FrozenOrderedNamedViewColumnSet)
        assert all(isinstance(col, NamedViewColumnABC) for col in column_set)
        self._named_view_columns = column_set

    @property
    def _base_column_set(self) -> FrozenOrderedNamedViewColumnSet:
        """ Returns a dictionary from name to View Column object
            (Override from `ViewWithColumnsABC`)

        Returns:
            FrozenNamedViewColumnSet: Frozen set of NamedViewColumn
        """
        return self._named_view_columns


class NamedView(NamedViewABC, ViewWithColumns):
    """ Named View class """


class ViewFinal(ViewABC):

    def __init__(self) -> None:
        super().__init__()
        self.__select_query: QueryData | None = None
        self.__result: TableData | None = None

    def _generate_select_query(self) -> QueryData:
        """ Generate QueryData for SELECT query """
        assert self._selected_exprs
        # print('self.base_view.select_from_query=', self.base_view.select_from_query)
        # assert self.base_view.select_from_query
        self._base_view._refresh_select_from_query()
        return QueryData(
            b'SELECT',
            [c.select_column_query for c in self._selected_exprs],
            b'FROM', self._base_view._select_from_query,
            (b'WHERE', self._where_expr) if self._where_expr is not NoneExpr else None,
            (b'GROUP', b'BY', [*self._groups]) if self._groups else None,
            (b'ORDER', b'BY', [c.ordered_query for c in self._orders]) if self._orders else None,
            (b'LIMIT', self._limit_value) if self._limit_value else None,
            (b'OFFSET', self._offset_value) if self._offset_value else None,
        )

    def _refresh_select_query(self) -> None:
        """ Refresh QueryData for SELECT query """
        self.__select_query = self._generate_select_query()

    @property
    def _view_name_or_none(self) -> ObjectName | None:
        return None  # This view has no name.

    @property
    def _select_query_or_none(self) -> QueryData | None:
        return self.__select_query

    def refresh_result(self) -> None:
        self.__result = self.db.query(self._select_query)

    @property
    def _result_or_none(self) -> TableData | None:
        return self.__result

    def join(self, join_type: JoinLike, view: ViewABC, expr: ExprABC) -> JoinedView:
        return JoinedView(self, JoinType.make(join_type), view, expr)
        
    def with_args(self, *argvals, **kwargvals) -> ViewWithArgs:
        return ViewWithArgs(self, *argvals, **kwargvals)

    def _new_view(self, *args, **kwargs) -> ViewABC:
        return CustomView(*args, **kwargs)
        

class ViewWithTarget(ViewWithTargetABC):
    """ View with the other view object as a target """
    
    def __init__(self, target_view) -> None:
        super().__init__()
        self.__target_view = target_view
        
    @property
    def _target_view(self) -> ViewABC:
        return self.__target_view


class JoinedView(ViewWithColumns, ViewWithTargetABC, ViewFinal):
    """ Table View """
    def __init__(self, dest_view: ViewABC, join_type: JoinLike, join_view: ViewABC, expr: ExprABC):

        # Check if a subquery is required for view to join
        if join_view._limit_value or join_view._offset_value:
            raise ObjectArgValueError('Cannot join a `join_view` because it needs subquery.'
                'Make a SubqueryView from `join_view` and specify it.', join_view)

        self.__target_view = dest_view
        self.__join_type = JoinType.make(join_type)
        self.__view_to_join = join_view
        self.__expr_for_join = expr

        if duplicate_columns := dest_view._base_column_set & join_view._base_column_set:
            raise ObjectError('Duplicate column names:', duplicate_columns)

        super().__init__(dest_view._base_column_set | join_view._base_column_set)

        self.__selected_exprs = self._merge_selected_exprs(
            dest_view._selected_exprs, join_view._selected_exprs,
            alias_format=join_view._column_alias_format)

        self.__select_from_query: QueryData | None = None


    def _merge_selected_exprs(self,
        exprs1: FrozenOrderedExprObjectSet,
        exprs2: FrozenOrderedExprObjectSet,
        alias_format: ObjectName,
    ) -> FrozenOrderedExprObjectSet:
        _selected_exprs = OrderedExprObjectSet(exprs1)
        for expr in exprs2:
            if expr in _selected_exprs:
                continue
            if (expr_name := expr.get_name()) in _selected_exprs:
                new_name = alias_format % expr_name
                _selected_exprs.add(AliasedExpr(expr, new_name))
            else:
                _selected_exprs.add(expr)
        return FrozenOrderedExprObjectSet(_selected_exprs)

    
    @property
    def _target_view(self) -> ViewABC:
        """ Get a destination View """
        return self.__target_view

    @property
    def _selected_exprs(self) -> FrozenOrderedExprObjectSet:
        return self.__selected_exprs

    @property
    def _join_type(self) -> JoinType:
        return self.__join_type

    @property
    def _view_to_join(self) -> ViewABC:
        if self.__view_to_join is None:
            raise ObjectNotSetError('View to join is not set.')
        return self.__view_to_join

    @property
    def _expr_for_join(self) -> ExprABC:
        if self.__expr_for_join is None:
            raise ObjectNotSetError('Expr for join is not set.')
        return self.__expr_for_join

    @property
    def _orders(self) -> FrozenOrderedExprObjectSet:
        return self._target_view._orders & self._view_to_join._outer_orders

    @property
    def _outer_orders(self) -> FrozenOrderedExprObjectSet:
        return self._orders

    def _refresh_select_from_query(self) -> None:
        """ Refresh QueryData for SELECT FROM """
        on_expr = (self._expr_for_join & self._view_to_join._where_expr)
        # print('on_expr = ', on_expr)
        self._target_view._base_view._refresh_select_from_query()
        target_from_query = self._target_view._base_view._select_from_query
        assert target_from_query is not None
        self.__select_from_query = QueryData(
            b'(', target_from_query, (
                self._join_type, b'JOIN',
                self._view_to_join._base_view._select_from_query,
                (b'ON', on_expr) if on_expr is not NoneExpr else None
            ), b')'
        )

    @property
    def _select_from_query_or_none(self) -> QueryData | None:
        return self.__select_from_query

    def __repr__(self) -> str:
        return ('JoinV(%s %s %s)'
            % (self._target_view, self._join_type.name, self._view_to_join))


class SubqueryView(NamedView):
    """ Subquery View """
    def __init__(self, target_view: ViewABC) -> None:
        self.__target_view = target_view
        super().__init__(FrozenOrderedNamedViewColumnSet(
            NamedViewColumn(self, col.get_name(), AnySQLType) # TODO: Fix type
            for col in target_view._selected_exprs))

    @property
    def _target_view(self) -> ViewABC:
        return self.__target_view

    def append_to_query_data(self, qd: QueryData) -> None:
        qd += self._view_name

    def _refresh_select_from_query(self) -> None:
        return self._target_view._refresh_select_query()

    @property
    def _select_from_query_or_none(self) -> QueryData | None:
        return QueryData(
            b'(', self._target_view._select_query, b')',
            b'AS', self._target_view)

    def __repr__(self) -> str:
        return 'SqV(%s)' % self._target_view


class ViewWithArgs(ViewWithTarget, ViewFinal):
    """ Table View with arguments """
    def __init__(self, target_view: ViewABC, *argvals: ValueType, **kwargvals: ValueType):
        super().__init__(target_view)
        self.__argvals = tuple(argvals)
        self.__kwargvals = tuple(kwargvals.items())

    @property
    def _selected_exprs(self):
        return self._target_view._selected_exprs

    @property
    def _argvals(self):
        return self.__argvals

    @property
    def _kwargvals(self):
        return self.__kwargvals

    def _generate_select_query(self):
        target_qd = self._target_view._select_query
        return target_qd.call(*self._argvals, **dict(self._kwargvals))

    def __repr__(self) -> str:
        return ('VA(%s, %s, %s)'
            % (self._target_view, self._argvals, self._kwargvals))


class CustomView(CustomViewABC, ViewFinal):
    """ Table View """
    def __init__(self,
        base_view: BaseViewABC,
        *column_likes: ColumnArgTypes,
        where    : ExprABC = NoneExpr,
        groups   : Iterable[NameLike | ExprObjectABC] = (),
        orders   : Iterable[OrderedColumnArgTypes] = (),
        limit    : ExprLike | None = None,
        offset   : ExprLike | None = None,
        outer_orders   : Iterable[OrderedExprObject] | None = None,
        **options,
    ):
        super().__init__(base_view)

        assert isinstance(base_view, BaseViewABC)
        assert isinstance(where, ExprABC)
        assert all(isinstance(c, (bytes, str, ObjectName, ExprObjectABC)) for c in groups)
        assert all(isinstance(c, (bytes, str, ObjectName, ExprObjectABC, tuple)) for c in orders)
        assert outer_orders is None or all(isinstance(c, OrderedExprObject) for c in outer_orders)

        self.__selected_exprs = self._process_select_column_args(*column_likes)
        if not self._selected_exprs:
            raise ObjectArgValueError('Columns cannot be empty.')
            
        self.__where_expr = where
        self.__groups = FrozenOrderedExprObjectSet(self._to_column(c) for c in groups)
        self.__orders = (
            FrozenOrderedExprObjectSet(self._process_order_args(*orders)) | base_view._outer_orders)

        self.__limit_value  = limit
        self.__offset_value = offset

        self.__outer_orders = self.__orders if outer_orders is None else FrozenOrderedExprObjectSet(outer_orders)
        self.__options = options

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
                if (column := self._base_view._to_column_or_none(expr)) is None:
                    raise ObjectNotFoundError('Column not found.', expr)
                new_expr = AliasedExpr(column, name)
            
            # ColumnABC or NameLike:
            #   Get a column/expr object from the base view
            elif isinstance(column_like, (bytes, str, ObjectName)):
                if (column := self._base_view.get_column_or_none(column_like)) is None:
                    raise ObjectNotFoundError('Column not found.', column_like)
                new_expr = column

            else:
                raise ObjectArgTypeError('Invalid column type.', column_like)

            # Register the ViewColumn object to this view.
            #   If there is a same name one, raise exception.
            if (expr_name := new_expr.get_name()) in _selected_exprs:
                raise ObjectNameAlreadyExistsError('Name already exists.', expr_name)
            _selected_exprs.add(new_expr)

        return FrozenOrderedExprObjectSet(_selected_exprs)


    def _process_order_args(self, *column_orders: OrderedColumnArgTypes) -> Iterator[OrderedExprObject]:
        
        for col_order in column_orders:
            if isinstance(col_order, tuple) and len(col_order) == 2:
                expr, otype = col_order
                yield self._base_view._to_column(expr).ordered(otype)
            
            elif isinstance(col_order, (bytes, str, ObjectName)):
                _name = ObjectName(col_order)
                _otype = (
                    OrderType.ASC if _name.raw_name[0:1] == b'+' else 
                    OrderType.DESC if _name.raw_name[0:1] == b'-' else None)
                if _otype is not None:
                    _name_entity = _name.raw_name[1:]
                    yield self._base_view.get_column(_name_entity).ordered(_otype)
                else:
                    yield self._base_view.get_column(_name).ordered(OrderType.ASC)
            elif isinstance(col_order, ExprObjectABC):
                if isinstance(col_order, OrderedExprObject):
                    yield col_order
                else:
                    yield col_order.ordered(OrderType.ASC)
            else:
                raise ObjectArgTypeError('Invalid type.', col_order)

    @property
    def _selected_exprs(self) -> FrozenOrderedExprObjectSet:
        return self.__selected_exprs
        
    @property
    def _where_expr(self) -> ExprABC:
        """ Get a WHERE expression of this view

        Returns:
            ExprABC: WHERE expression
        """
        return self.__where_expr

    @property
    def _groups(self) -> FrozenOrderedExprObjectSet:
        """ Get GROUP BY grouping columns of this view

        Returns:
            FrozenOrderedExprObjectSet: Grouping columns
        """
        return self.__groups

    @property
    def _orders(self) -> FrozenOrderedExprObjectSet:
        """ Get ORDER BY ordering columns of this view

        Returns:
            FrozenOrderedExprObjectSet: Ordering columns
        """
        return self.__orders

    @property
    def _outer_orders(self) -> FrozenOrderedExprObjectSet:
        """ Get ORDER BY ordering columns for outer views

        Returns:
            FrozenOrderedExprObjectSet: Ordering columns
        """
        return self.__outer_orders

    @property
    def _limit_value(self) -> ExprLike | None:
        """ Get a LIMIT value of this view

        Returns:
            ExprLike | None: LIMIT value
                If not set, returns `None`.
        """
        return self.__limit_value

    @property
    def _offset_value(self) -> ExprLike | None:
        """ Get a OFFSET value of this view

        Returns:
            ExprLike | None: OFFSET value
                If not set, returns `None`.
        """
        return self.__offset_value

    def __repr__(self) -> str:
        return 'V(%s)' % repr(self._base_view)


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

    def _new_view(self, *args, **kwargs): # -> View:
        return CustomSingleView(*args, **kwargs)

    def __repr__(self) -> str:
        return 'SV(%s)' % repr(self._base_view)
