"""
    Table Record classes
"""
from __future__ import annotations
from abc import abstractmethod
from typing import TYPE_CHECKING, Generic, Mapping, TypeVar

if TYPE_CHECKING:
    from ...schema.abc.view import ViewABC
    from ...syntax.abc.object import ObjectName
    from ...syntax.exprs import ExprObjectABC


class RecordABC:
    """ Record ABC """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

        _names = set()
        _selected_exprs = self._view._selected_exprs

        for i, expr in enumerate(_selected_exprs):
            name = expr.get_name()

            _has_kwd_arg = name in kwargs
            _has_pos_arg = i < len(args)
            if _has_kwd_arg and _has_pos_arg:
                raise ValueError('Duplicate column value.', i, expr)

            if _has_kwd_arg or _has_pos_arg:
                value = kwargs[name] if _has_kwd_arg else args[i]
                if hasattr(self, name):
                    raise RuntimeError('Cannot overwrite attribute.', name)
                setattr(self, name, value)
                _names.add(name)

        _unused_args = args[len(_selected_exprs):]
        _unused_kwargs = [key for key in kwargs if key not in _names]
        if _unused_args or _unused_kwargs:
            raise ValueError('Unused arguments: ', _unused_args, _unused_kwargs)
            
        self._names = frozenset(_names)


    @abstractmethod
    def get_view(self) -> ViewABC:
        """ Get a parent view object """

    @property
    def _view(self):
        return self.get_view()

    @abstractmethod
    def asdict(self) -> Mapping['ObjectName', 'ColumnValue']:
        """ Get a data dictionary """
        return {name: getattr(self, name) for name in self._names}

    def __getitem__(self, name):
        if name in self:
            return getattr(self, name)

    def __contains__(self, name):
        return name in self._names


T = TypeVar('T')
class ColumnValue(Generic[T]):
    """ Cell (Value of column in record) """
    def __init__(self, record: RecordABC, expr: ExprObjectABC, v: T) -> None:
        super().__init__()
        self._record = record
        self._expr = expr
        self._v = v

    @property
    def expr(self) -> ExprObjectABC:
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

