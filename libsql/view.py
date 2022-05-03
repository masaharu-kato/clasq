"""
    SQL query module
"""

from typing import List, NamedTuple, Optional, Tuple, Union

from .syntax.keywords import JoinLike, JoinType, make_join_type
from .syntax.expr_type import ExprABC, NameLike, OP, ObjectABC
from .schema import ColumnLike, ViewABC, Table, ViewLike, Column, OrderedColumn, Database
from .utils.tabledata import TableData

JoinData = Tuple[ViewLike, JoinLike, Optional[ExprABC]]

class View(ViewABC):
    """ Table View """
    def __init__(self,
        *_columns: Optional[ObjectABC], 
        database : Optional[Database] = None,
        name     : Optional[NameLike] = None,
        froms    : Optional[Union[List[ViewLike], Tuple[ViewLike, ...]]] = None,
        joins    : Optional[Union[List[JoinData], Tuple[JoinData, ...]]] = None,
        where    : Optional[ExprABC] = None,
        groups   : Optional[Union[List[Column], Tuple[Column, ...]]] = None,
        orders   : Optional[Union[List[OrderedColumn], Tuple[OrderedColumn, ...]]] = None,
        limit    : Optional[int] = None,
        offset   : Optional[int] = None,
    ):
        super().__init__(name or b'', database, *_columns)
        self._froms : Tuple[Table, ...] = self._make_tuple(froms)
        self._joins  : Tuple[Tuple[Table, JoinType, ExprABC], ...] = self._make_tuple(joins)
        self._where  : Optional[ExprABC] = where
        self._groups : Tuple[Column, ...] = self._make_tuple(groups)
        self._orders : Tuple[OrderedColumn, ...] = self._make_tuple(orders)
        self._limit  : Optional[int] = limit
        self._offset : Optional[int] = offset

        self._result : Optional[TableData] = None

    # ----------------------------------------------------------------
    #   View creation methods
    # ----------------------------------------------------------------

    def clone(self,
        *_columns: Optional[ObjectABC], 
        database : Optional[Database] = None,
        name     : Optional[NameLike] = None,
        froms    : Optional[Union[List[ViewLike], Tuple[ViewLike, ...]]] = None,
        joins    : Optional[Union[List[JoinData], Tuple[JoinData, ...]]] = None,
        where    : Optional[ExprABC] = None,
        groups   : Optional[Union[List[Column], Tuple[Column, ...]]] = None,
        orders   : Optional[Union[List[OrderedColumn], Tuple[OrderedColumn, ...]]] = None,
        limit    : Optional[int] = None,
        offset   : Optional[int] = None,
    ) -> 'View':

        return self._new(
            *self.iter_columns(), *_columns,
            database = database if database is not None else self._database,
            name = name if name is not None else self._name,
            froms = self._join_tuple(self._froms, froms),
            joins = self._join_tuple(self._joins, joins),
            where = OP.AND.call_joined_opt(self._where, where),
            groups = self._join_tuple(self._groups, groups),
            orders = self._join_tuple(self._orders, orders),
            limit = limit if limit is not None else self._limit,
            offset = offset if offset is not None else self._offset,
        )

    def merged(self, view: 'View') -> 'View':
        return self.clone(
            *view.iter_columns(),
            database = view._database,
            name   = view._name,
            froms  = view._froms,
            joins  = view._joins,
            where  = view._where,
            groups = view._groups,
            orders = view._orders,
            limit  = view._limit,
            offset = view._offset,
        )

    def select_column(self, *_columns: Optional[ObjectABC]) -> 'View':
        """ Make a View object with columns """
        return self.clone(*_columns)

    def where(self, *exprs, **coleqs) -> 'View':
        """ Make a View object with WHERE clause """
        return self.clone(where=self._proc_terms(*exprs, **coleqs))

    def group_by(self, *columns: Column) -> 'View':
        return self.clone(groups=columns)

    def order_by(self, *orders: OrderedColumn) -> 'View':
        """ Make a View object with ORDER BY clause """
        return self.clone(orders=orders)

    def limit(self, limit: int) -> 'View':
        """ Make a View object with LIMIT OFFSET clause """
        return self.clone(limit=limit)

    def offset(self, offset: int) -> 'View':
        """ Make a View object with LIMIT OFFSET clause """
        return self.clone(offset=offset)

    # def single(self) -> 'SingleView':
    #     """ Make a View object with a single result """
    #     return SingleView(self)

    def join(self, join_type: JoinLike, view: ViewLike, *exprs: ExprABC, **_coleqs: ColumnLike):
        """ Make a Joined View """
        coleqs = {n: self.column(v) for n, v in _coleqs.items()}
        return self.clone(joins=[(
            self._proc_view(view),
            make_join_type(join_type),
            self._proc_terms(*exprs, **coleqs)
        )])

    def inner_join(self, view: ViewLike, *exprs: ExprABC, **coleqs: ColumnLike):
        """ Make a INNER Joined View """
        return self.join(JoinType.INNER, view, *exprs, **coleqs)

    def left_join(self, view: ViewLike, *exprs: ExprABC, **coleqs: ColumnLike):
        """ Make a LEFT Joined View """
        return self.join(JoinType.LEFT, view, *exprs, **coleqs)

    def right_join(self, view: ViewLike, *exprs: ExprABC, **coleqs: ColumnLike):
        """ Make a RIGHT Joined View """
        return self.join(JoinType.RIGHT, view, *exprs, **coleqs)

    def outer_join(self, view: ViewLike, *exprs: ExprABC, **coleqs: ColumnLike):
        """ Make a OUTER Joined View """
        return self.join(JoinType.OUTER, view, *exprs, **coleqs)

    def cross_join(self, view: ViewLike, *exprs: ExprABC, **coleqs: ColumnLike):
        """ Make a CROSS Joined View """
        return self.join(JoinType.CROSS, view, *exprs, **coleqs)

    def __getitem__(self, val):

        if isinstance(val, int):
            return self.clone(offset=val, limit=1)

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
            return super().__getitem__(val)
        
        if isinstance(val, ExprABC):
            return self.clone(where=val)

        raise TypeError('Invalid type %s (%s)' % (type(val), val))

    
    def refresh_result(self) -> None:
        self._result = self.cnx.select(
            *self.iter_columns(),
            froms  = self._froms,
            joins  = self._joins,
            where  = self._where,
            groups = self._groups,
            orders = self._orders,
            limit  = self._limit,
            offset = self._offset,
        )

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

    @classmethod
    def _make_tuple(cls, t: Optional[Union[tuple, list]]) -> tuple:
        if t is None:
            return ()
        if isinstance(t, tuple):
            return t
        return tuple(t)

    @classmethod
    def _join_tuple(cls, t1: Optional[Union[tuple, list]], t2: Optional[Union[tuple, list]]) -> tuple:
        if t1 is None:
            return cls._make_tuple(t2)
        if t2 is None:
            return cls._make_tuple(t1)
        return (*t1, *t2)

    def _new(self, *args, **kwargs) -> 'View':
        return View(*args, **kwargs)

    def _proc_view(self, viewlike: ViewLike) -> ViewABC:
        if isinstance(viewlike, ViewABC):
            return viewlike
        return self.database[viewlike]

    def _proc_terms(self, *exprs: Optional[ExprABC], **coleqs: ExprABC) -> Optional[ExprABC]:
        return OP.AND.call_joined_opt(
            OP.AND.call_joined_opt(*exprs),
            OP.AND.call_joined_opt(*(self.column(n) == v for n, v in coleqs.items()))
        )


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

    def _new(self, *args, **kwargs) -> 'View':
        return SingleView(*args, **kwargs)
