"""
    Table Record classes
"""
from abc import ABC, abstractmethod, abstractproperty
from typing import TYPE_CHECKING, Dict, Generic, Mapping, Optional, TypeVar, get_type_hints

if TYPE_CHECKING:
    from ..syntax.exprs import ExprObjectABC
    from ..syntax.object_abc import ObjectName
    from ..schema.view_abc import ViewABC


class RecordABC:
    """ Record ABC """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()
        self._vals = {expr.name: Cell(self, expr, arg) for expr, arg in zip(self.view.selected_exprs, args)}
        for name, arg in kwargs.items():
            expr = self.view.column(name)
            assert expr.name not in self._vals
            self._vals[expr.name] = Cell(self, expr, arg)

    @abstractproperty
    def view(self) -> 'ViewABC':
        """ Get a parent view object """

    def asdict(self) -> Mapping['ObjectName', 'Cell']:
        """ Get a data dictionary """
        return self._vals


T = TypeVar('T')
class Cell(Generic[T]):
    """ Cell (Value of column in record) """
    def __init__(self, record: RecordABC, expr: 'ExprObjectABC', v: T) -> None:
        super().__init__()
        self._record = record
        self._expr = expr
        self._v = v

    @property
    def expr(self) -> 'ExprObjectABC':
        """ Get a parent column object """
        return self._expr

    @property
    def record(self) -> RecordABC:
        """ Get a parent record object """
        return self._record

    @property
    def value(self) -> T:
        """ Get an actual value """
        return self._v

