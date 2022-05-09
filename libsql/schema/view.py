"""
    View classes
"""
from abc import abstractmethod, abstractproperty
from typing import TYPE_CHECKING, Dict, Iterable, Optional, Tuple, Union, overload

from ..syntax.object_abc import Object, Name, OrderedObjectSet, to_name, FrozenObjectSet, OrderedFrozenObjectSet
from ..syntax.exprs import ExprABC, NoneExpr, OP, NamedExprABC, NamedExpr
from ..syntax.keywords import JoinType, JoinLike, OrderLike
from ..syntax.query_data import QueryData
from ..syntax import errors
from ..utils.tabledata import TableData

if TYPE_CHECKING:
    from .database import Database
    from ..connection import ConnectionABC


class ViewABC(Object):
    """ View Expr """

    def __init__(self, name: Name):
        super().__init__(name)
        if not self.name:
            raise errors.ObjectArgsError('View name cannot be empty.')

        self._qd_table_expr: Optional[QueryData] = None
        self._qd_select : Optional[QueryData] = None # SELECT query data
        self._result : Optional[TableData] = None # View Result

    @abstractproperty
    def base_view(self) -> 'ViewABC':
        """ Get a base View object """

    @abstractproperty
    def available_named_exprs(self) -> FrozenObjectSet[NamedExprABC]:
        """ Iterate available NamedExpr objects """
    
    @abstractproperty
    def named_exprs(self) -> OrderedFrozenObjectSet[NamedExprABC]:
        """ Iterate NamedExpr objects """
        
    @property
    def nexprs(self) -> OrderedFrozenObjectSet[NamedExprABC]:
        """ Iterate NamedExpr objects (Synonym of `named_exprs`) """
        return self.named_exprs

    @abstractproperty
    def query_table_expr(self) -> QueryData:
        """ QueryData for SELECT FROM """

    @abstractproperty
    def outer_named_exprs(self) -> OrderedFrozenObjectSet[NamedExprABC]:
        """ NamedExpr objects for Outer View (which joins this view) """

    @abstractmethod
    def column(self, val: Union[Name, NamedExprABC]) -> NamedExprABC:
        """ Get a NamedExpr object with the specified name
                or check existing NamedExpr object is valid for this Table object

        Args:
            val (bytes | str | NamedExprABC): NamedExpr name or Column object

        Returns:
            NamedExprABC: NamedExpr object with the specified name or Column object itself
        """

    # @abstractmethod
    # def append_named_expr_object(self, nexpr: NamedExprABC) -> None:
    #     """ Append (existing) named expression object

    #     Args:
    #         nexpr (NamedExprABC): Named expression object
    #     """

    @abstractmethod
    def _new_view(self, *args, **kwargs) -> 'ViewABC':
        """ Make a new view with arguments """

    def __repr__(self):
        return 'View(%s)' % str(self)

    @property
    def database_or_none(self) -> Optional['Database']:
        return self.base_view.database_or_none # Default Implementation

    @property
    def database(self) -> 'Database':
        """ Get a Database object of this view """
        if self.database_or_none is None:
            raise errors.ObjectNotSetError('Database is not set.')
        return self.database_or_none

    @property
    def db(self) -> 'Database':
        return self.database

    @property
    def cnx(self) -> 'ConnectionABC':
        return self.database.cnx

    @property
    def exists_on_db(self) -> bool:
        return False # Default Implementation

    def col(self, val: Union[Name, NamedExprABC]):
        """ Synonym of `column` method """
        return self.column(val)

    def column_or_none(self, val: Union[Name, NamedExprABC]) -> Optional[NamedExprABC]:
        """ Get a Column object with the specified name if exists """
        try:
            return self.column(val)
        except (errors.ObjectNotFoundError, errors.NotaSelfObjectError):
            pass
        return None

    def get(self, val: Union[Name, NamedExprABC]) -> Optional[NamedExprABC]:
        """ Synonym of `column_or_none` method """
        return self.column_or_none(val)

    @property
    def join_type(self) -> Optional[JoinType]:
        return None # Default Implementation

    @property
    def view_to_join(self) -> 'ViewABC':
        raise errors.ObjectNotSetError('Join is not set.') # Default Implementation

    @property
    def expr_for_join(self) -> 'ExprABC':
        raise errors.ObjectNotSetError('Join is not set.') # Default Implementation

    @property
    def where_expr(self) -> ExprABC:
        return NoneExpr  # Default Implementation

    @property
    def where_expr_for_join(self):
        return NoneExpr  # Default Implementation

    @property
    def groups(self) -> Iterable[NamedExprABC]:
        return () # Default Implementation

    @property
    def orders(self) -> Iterable[NamedExprABC]:
        return () # Default Implementation

    @property
    def outer_orders(self) -> Iterable[NamedExprABC]:
        return () # Default Implementation

    @property
    def limit_value(self) -> Optional[int]:
        return None # Default Implementation

    @property
    def offset_value(self) -> Optional[int]:
        return None # Default Implementation

    @property
    def argvals(self) -> Iterable[ValueType]:
        return () # Default Implementation

    @property
    def kwargvals(self) -> Iterable[Tuple[str, ValueType]]:
        return () # Default Implementation

    @property
    def force_join_subquery(self) -> bool:
        return False # Default Implementation

    @property
    def subquery_required_for_join(self) -> bool:
        return self.force_join_subquery or bool(self.groups or self.limit_value or self.offset_value)

    @property
    def expr_for_outer_join(self) -> ExprABC:
        return NoneExpr if self.subquery_required_for_join else self.where_expr  # Default Implementation

    @property
    def query_table_expr_for_join(self) -> QueryData:
        if self.subquery_required_for_join: # Not working
            return QueryData(b'(', self.query_select, b')', b'AS', self.name)
        return self.base_view.query_table_expr

    def refresh_query_select(self) -> QueryData:
        """ Refresh QueryData """
        qd = QueryData(
            b'SELECT',
            [c.query_for_select_column for c in self.named_exprs] if self.named_exprs else b'*',
            b'FROM', self.query_table_expr,
            (b'WHERE', self.where_expr) if self.where_expr is not NoneExpr else None,
            (b'GROUP', b'BY', [*self.groups]) if self.groups else None,
            (b'ORDER', b'BY', [c.q_order() for c in self.orders]) if self.orders else None,
            (b'LIMIT', self.limit_value) if self.limit_value else None,
            (b'OFFSET', self.offset_value) if self.offset_value else None,
        )
        self._qd_select = qd
        return qd

    @property
    def query_select(self) -> QueryData:
        if self._qd_select is None:
            return self.refresh_query_select()
        return self._qd_select

    def clone(self,
        *,
        nexprs: Optional[Iterable[NamedExprABC]] = None,
        name  : Optional[Name] = None,
        join_type: Optional[JoinLike] = None,
        join_view: Optional['ViewABC'] = None,
        join_expr: Optional[ExprABC] = None,
        where : ExprABC = NoneExpr,
        groups: Iterable[NamedExprABC] = (),
        orders: Iterable[NamedExprABC] = (),
        limit : Optional[int] = None,
        offset: Optional[int] = None,
        force_join_subquery: bool = False,
    ) -> 'ViewABC':
        # print('Clone to a new view ...')
        assert self.named_exprs
        new_view = self._new_view(  
            base_view = self.base_view,
            nexprs = nexprs if nexprs is not None else self.nexprs,
            name = name if name is not None else self._name,
            join_type = join_type,
            join_view = join_view,
            join_expr = join_expr,
            where = self.where_expr & where,
            groups = (*self.groups, *groups),
            orders = (*self.orders, *orders),
            limit = limit if limit is not None else self.limit_value,
            offset = offset if offset is not None else self.offset_value,
            force_join_subquery = force_join_subquery,
        )
        if join_type is not None:
            # print('Return the new view with base.')
            return self._new_view(base_view=new_view)
        # print('Retrun the new view.')
        return new_view

    # def merged(self, view: 'View') -> 'ViewABC':
    #     return self.clone(
    #         *view.iter_named_exprs(),
    #         name = view._name,
    #         base = self.base_view
    #         joins  = view.joins,
    #         where  = view.where_expr,
    #         groups = view.groups,
    #         orders = view.orders,
    #         limit  = view.limit_value,
    #         offset = view.offset_value,
    #     )

    def select_view_column(self, view: 'ViewABC', *cols: NamedExprABC, **as_cols: NamedExprABC) -> 'ViewABC':
        """ Make a View object with specific columns (NamedExpr objects) """
        return self.clone(nexprs=(self.nexprs - view.nexprs) | self._proc_col_args(*cols, **as_cols))

    def select_table_column(self, view: 'ViewABC', *cols: NamedExprABC, **as_cols: NamedExprABC) -> 'ViewABC':
        """ Make a View object with specific columns (NamedExpr objects) """
        return self.select_view_column(view, *cols, **as_cols)

    def select_column(self, *cols: NamedExprABC, **as_cols: NamedExprABC) -> 'ViewABC':
        """ Make a View object with specific columns (NamedExpr objects) """
        return self.clone(nexprs=self._proc_col_args(*cols, **as_cols))

    def add_column(self, *cols: NamedExprABC, **as_cols: NamedExprABC) -> 'ViewABC':
        """ Make a View object with additional columns (NamedExpr objects) """
        return self.clone(nexprs=self.nexprs | self._proc_col_args(*cols, **as_cols))

    def where(self, *exprs, **coleqs) -> 'ViewABC':
        """ Make a View object with WHERE clause """
        return self.clone(where=OP.AND(*exprs, **coleqs))

    def group_by(self, *columns: NamedExprABC, **cols: Optional[bool]) -> 'ViewABC':
        return self.clone(groups=[
            *columns,
            *(self.column(c) for c, v in cols.items() if v)
        ])

    def order_by(self, *orders: NamedExprABC, **col_orders: Optional[OrderLike]) -> 'ViewABC':
        """ Make a View object with ORDER BY clause """
        return self.clone(orders=[
            *orders,
            *(self.column(c).ordered(v) for c, v in col_orders.items() if v is not None)
        ])

    def limit(self, limit: int) -> 'ViewABC':
        """ Make a View object with LIMIT OFFSET clause """
        return self.clone(limit=limit)

    def offset(self, offset: int) -> 'ViewABC':
        """ Make a View object with LIMIT OFFSET clause """
        return self.clone(offset=offset)

    # def single(self) -> 'SingleView':
    #     """ Make a View object with a single result """
    #     return SingleView(self)

    def join(self, join_type: JoinLike, view: 'ViewABC', expr: ExprABC) -> 'ViewABC':
        """ Make a Joined View """
        return self.clone(
            join_type = JoinType.make(join_type),
            join_view = view,
            join_expr = expr,
        )

    def inner_join(self, view: 'ViewABC', expr: ExprABC) -> 'ViewABC':
        """ Make a INNER Joined View """
        return self.join(JoinType.INNER, view, expr)

    def left_join(self, view: 'ViewABC', expr: ExprABC) -> 'ViewABC':
        """ Make a LEFT Joined View """
        return self.join(JoinType.LEFT, view, expr)

    def right_join(self, view: 'ViewABC', expr: ExprABC) -> 'ViewABC':
        """ Make a RIGHT Joined View """
        return self.join(JoinType.RIGHT, view, expr)

    def outer_join(self, view: 'ViewABC', expr: ExprABC) -> 'ViewABC':
        """ Make a OUTER Joined View """
        return self.join(JoinType.OUTER, view, expr)

    def cross_join(self, view: 'ViewABC', expr: ExprABC) -> 'ViewABC':
        """ Make a CROSS Joined View """
        return self.join(JoinType.CROSS, view, expr)

    @overload
    def __getitem__(self, val: Union[int, slice, ExprABC, Tuple[ExprABC, ...]]) -> 'ViewABC': ...

    @overload
    def __getitem__(self, val: Union[bytes, str]) -> NamedExpr: ...

    @overload
    def __getitem__(self, val: Tuple[Union[bytes, str], ...]) -> Tuple[NamedExpr, ...]: ...
        
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

        if isinstance(val, (bytes, str)):
            return self.column(val)
        
        if isinstance(val, ExprABC):
            return self.where(val)

        if isinstance(val, tuple):
            if all(isinstance(v, (bytes, str)) for v in val):
                return (*(self.column(v) for v in val),)
            if all(isinstance(v, ExprABC) for v in val):
                return self.where(*val)
            raise errors.ObjectArgTypeError('Invalid tuple value type.', val)

        raise errors.ObjectArgTypeError('Invalid type.', val)

    def with_args(self, *argvals, **kwargvals) -> 'ViewABC':
        return self.clone(argvals=argvals, kwargvals=kwargvals.items())

    def result_with_args(self, *argvals, **kwargvals) -> TableData:
        return self.with_args(*argvals, **kwargvals).result

    def __call__(self, *argvals, **kwargvals) -> TableData:
        return self.result_with_args(*argvals, **kwargvals)
    
    def refresh_result(self) -> None:
        self._result = self.db.query_qd(self.query_select)

    def prepare_result(self) -> None:
        if self._result is None:
            return self.refresh_result()

    @property
    def is_result_ready(self) -> bool:
        return self._result is not None

    @property
    def result(self):
        self.prepare_result()
        assert self._result is not None
        return self._result

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

    def check_eq(self, value: 'ViewABC') -> bool:
        if not isinstance(value, ViewABC):
            raise TypeError('Invalid value type.')
        view = value
        return (
            self.named_exprs == view.named_exprs
            and self._name == view._name
            and self.base_view == view.base_view
            and self.join_type == view.join_type
            and self.view_to_join == view.view_to_join
            and self.expr_for_join == view.expr_for_join
            and self.where_expr  == view.where_expr
            and self.groups == view.groups
            and self.orders == view.orders
            and self.limit_value  == view.limit_value
            and self.offset_value == view.offset_value
        )

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

    def _proc_view(self, viewlike: Union['ViewABC', Name]) -> 'ViewABC':
        if isinstance(viewlike, ViewABC):
            return viewlike
        return self.database[viewlike]

    def _proc_col_args(self, *cols: NamedExprABC, **as_cols: NamedExprABC) -> OrderedFrozenObjectSet[NamedExprABC]:
        return OrderedFrozenObjectSet(*cols, *(c.as_(n) for n, c in as_cols.items()))


class View(ViewABC):
    """ Table View """
    def __init__(self,
        *,
        base_view: ViewABC,
        nexprs   : Iterable[NamedExprABC] = (),
        join_type: Optional[JoinLike] = None,
        join_view: Optional[ViewABC] = None,
        join_expr: Optional[ExprABC] = None,
        name     : Optional[Name] = None,
        where    : ExprABC = NoneExpr,
        groups   : Iterable[NamedExprABC] = (),
        orders   : Iterable[NamedExprABC] = (),
        limit    : Optional[int] = None,
        offset   : Optional[int] = None,
        outer_orders   : Optional[Iterable[NamedExprABC]] = None,
        outer_nexprs   : Optional[Iterable[NamedExprABC]] = None,
        argvals: Iterable[ValueType] = (),
        kwargvals: Iterable[Tuple[str, ValueType]] = (),
        force_join_subquery: bool = False,
        column_alias_format: Name = b'%s',
        dynamic: bool = False,
        exists_on_db: bool = False,
        **options,
    ):
        super().__init__(name if name is not None else base_view.name)

        if join_type is not None and not (isinstance(join_view, ViewABC) and isinstance(join_expr, ExprABC)):
            raise errors.ObjectArgsError('Join view or Join expr is not set.')

        self._base_view = base_view
        
        self._join_type = JoinType.make(join_type) if join_type is not None else join_type
        self._view_to_join = join_view
        self._expr_for_join = join_expr

        self._named_exprs = OrderedFrozenObjectSet(
            *(nexprs or list(self.base_view.outer_named_exprs)),
            *(list(self._view_to_join.outer_named_exprs) if self._view_to_join else ()),
        )
        
        # print('arg nexprs=', nexprs)
        # print('self nexprs=', self._named_exprs)
        # print('base outer nexprs=', self.base_view.outer_named_exprs)
        if not self._named_exprs:
            raise errors.ObjectNotSetError('NamedExprs are empty.')

        self._where = where
        self._groups = OrderedFrozenObjectSet(*groups)
        self._orders = OrderedFrozenObjectSet(*orders, *base_view.outer_orders)
        self._limit  = limit
        self._offset = offset

        self._outer_named_exprs  = self._named_exprs if outer_nexprs is None else OrderedFrozenObjectSet(*outer_nexprs)
        # print('outer_nexprs=', self._outer_named_exprs)
        self._outer_orders  = orders if outer_orders is None else tuple(outer_orders)
        self._force_join_subquery = force_join_subquery
        self._column_alias_format = to_name(column_alias_format)
        self._exists_on_db = exists_on_db
        self._dynamic = dynamic
        self._options = options

        # Calculate all available NamedExpr objects (Virtual Columns)
        self._available_nexprs = FrozenObjectSet(
            *list(base_view.available_named_exprs),
            *(list(join_view.available_named_exprs) if join_view is not None else ())
        )

        for nexpr in self._named_exprs:
            if nexpr not in self._available_nexprs and not nexpr.consists_of(self._available_nexprs):
                raise errors.ObjectExprError('NamedExpr not consists of available columns.', nexpr)

        # Generate a dictionary from expression name to expression
        self._nexpr_dict: Dict[bytes, NamedExprABC] = {}
        for nexpr in self.named_exprs:
            if nexpr.name not in self._nexpr_dict:
                self._nexpr_dict[nexpr.name] = nexpr
            # alias_name = self._column_alias_format % nexpr.name
            # if alias_name in self._nexpr_dict:
            #     raise errors.ObjectNameAlreadyExistsError('Alias already exists.', alias_name)
            # self._nexpr_dict[alias_name] = nexpr

        self._argvals = tuple(argvals)
        self._kwargvals = tuple(kwargvals)


    @property
    def base_view(self) -> 'ViewABC':
        """ Get a base View object """
        return self._base_view

    @property
    def available_named_exprs(self) -> FrozenObjectSet[NamedExprABC]:
        return self._available_nexprs

    @property
    def named_exprs(self) -> OrderedFrozenObjectSet[NamedExprABC]:
        """ NamedExpr objects in this View """
        return self._named_exprs

    @property
    def outer_named_exprs(self) -> OrderedFrozenObjectSet[NamedExprABC]:
        """ NamedExpr objects for Outer View (which joins this view) """
        return self._outer_named_exprs

    @property
    def join_type(self) -> Optional[JoinType]:
        return self._join_type

    @property
    def view_to_join(self) -> 'ViewABC':
        if self._view_to_join is None:
            raise errors.ObjectNotSetError('View to join is not set.')
        return self._view_to_join

    @property
    def expr_for_join(self) -> 'ExprABC':
        if self._expr_for_join is None:
            raise errors.ObjectNotSetError('Expr for join is not set.')
        return self._expr_for_join

    @property
    def where_expr(self) -> ExprABC:
        return self._where

    @property
    def groups(self) -> Iterable[NamedExprABC]:
        return self._groups

    @property
    def orders(self) -> Iterable[NamedExprABC]:
        return self._orders

    @property
    def outer_orders(self) -> Iterable[NamedExprABC]:
        """ NamedExpr objects for Outer View (which joins this view) """
        return self._outer_orders

    @property
    def limit_value(self) -> Optional[int]:
        return self._limit

    @property
    def offset_value(self) -> Optional[int]:
        return self._offset

    @property
    def force_join_subquery(self) -> bool:
        return self._force_join_subquery

    @property
    def is_dynamic(self):
        return self._dynamic

    def refresh_query_table_expr(self) -> QueryData:
        """ Refresh QueryData for table FROM """
        if self.join_type is not None:
            on_expr = self.expr_for_join & self.view_to_join.expr_for_outer_join
            # print('on_expr = ', on_expr)
            qd = QueryData(
                b'(', self.base_view.query_table_expr, (
                    self.join_type, b'JOIN', self.view_to_join.query_table_expr_for_join,
                    (b'ON', on_expr) if on_expr is not NoneExpr else None
                ), b')'
            )
        else:
            qd = QueryData(self.base_view.query_table_expr)
        self._qd_table_expr = qd
        return qd

    @property
    def query_table_expr(self) -> QueryData:
        if self._qd_table_expr is None:
            return self.refresh_query_table_expr()
        return self._qd_table_expr

    @property
    def argvals(self):
        return self._argvals

    @property
    def kwargvals(self):
        return self._kwargvals

    def column(self, val: Union[Name, NamedExprABC]) -> NamedExprABC:
        """ Get a Column object with the specified name
                or check existing Column object is valid for this Table object
            (Overrided from `ViewABC`)

        Args:
            val (bytes | str | NamedExprABC): Column name or Column object

        Raises:
            errors.ObjectNotFoundError: _description_
            errors.NotaSelfObjectError: _description_
            errors.ObjectArgTypeError: _description_

        Returns:
            Column: Column object with the specified name or Column object itself
        """
        if isinstance(val, (bytes, str)):
            name = to_name(val)
            if name not in self._nexpr_dict:
                if not self.is_dynamic:
                    raise errors.ObjectNotFoundError('Undefined column name `%r` on view `%r`' % (name, self._name))
                self._nexpr_dict[name] = NamedExpr(name)
            return self._nexpr_dict[name]
            
        if isinstance(val, NamedExprABC):
            if not val in self._nexpr_dict.values():
                if not self.is_dynamic:
                    raise errors.NotaSelfObjectError('Not a column of this view.', val)
                self._nexpr_dict[val.name] = val
            return val

        raise errors.ObjectArgTypeError('Invalid type.', val)

    def append_named_expr_object(self, nexpr: NamedExprABC) -> None:
        """ Append (existing) named expression object
            (Overrided from `ViewABC`)

        Args:
            nexpr (NamedExprABC): Named expression object
        """
        if nexpr.name in self._nexpr_dict:
            raise errors.ObjectNameAlreadyExistsError('NamedExpr name already exists.', nexpr)
        self._nexpr_dict[nexpr.name] = nexpr

    def _new_view(self, *args, **kwargs) -> 'ViewABC':
        return View(*args, **kwargs)


class SingleView(View):
    """ Single result view """

    @property
    def result(self):
        return super().result[0]

    def __dict__(self):
        return dict(self.result)

    def __iter__(self):
        return iter(self.result)

    def __getitem__(self, val):

        if isinstance(val, (bytes, str)):
            val = val if isinstance(val, bytes) else val.encode()
            return self.result[val]

        return super().__getitem__(val)

    def _new_view(self, *args, **kwargs) -> 'View':
        return SingleView(*args, **kwargs)

