"""
    Foreign Key Reference
"""
from __future__ import annotations

from ...syntax.abc.object import NameLike, ObjectName
from ...syntax.abc.query import QueryABC, QueryDataABC
from ...syntax.abc.keywords import ReferenceOption
from .table import TableColumnABC


class ForeignKeyReference(QueryABC):
    """ Foreign Key Reference """

    def __init__(self,
        orig_column: TableColumnABC | tuple[TableColumnABC, ...],
        ref_column : TableColumnABC | tuple[TableColumnABC, ...],
        *,
        on_delete: ReferenceOption | None = None,
        on_update: ReferenceOption | None = None,
        name: NameLike = None,
    ):  
        self.__name = ObjectName(name) if name is not None else None

        _orig_columns = orig_column if isinstance(orig_column, (tuple, list)) else [orig_column]
        _ref_columns  = ref_column  if isinstance(ref_column , (tuple, list)) else [ref_column]
        assert len(_orig_columns) and _orig_columns[0].table is not None
        assert len(_ref_columns ) and _ref_columns [0].table is not None

        self._orig_table = _orig_columns[0].table
        self._ref_table = _ref_columns[0].table
        assert all(self._orig_table == c.table for c in _orig_columns)
        assert all(self._ref_table  == c.table for c in _ref_columns)

        self._orig_columns = orig_column if isinstance(orig_column, (tuple, list)) else [orig_column]
        self._ref_columns  = ref_column  if isinstance(ref_column , (tuple, list)) else [ref_column]
        self._on_delete = on_delete
        self._on_update = on_update

    @property
    def on_delete(self):
        return self._on_delete

    @property
    def on_update(self):
        return self._on_update

    @property
    def _name_or_none(self) -> ObjectName | None:
        return self.__name

    def _append_to_query_data(self, qd: QueryDataABC) -> None:
        """ Append this to query data"""
        qd.append(
            b'FOREIGN', b'KEY', self._name_or_none or (), b'(', [c._name for c in self._orig_columns], b')',
            b'REFERENCES', self._ref_table, b'(', [c._name for c in self._ref_columns], b')',
            (b'ON', b'DELETE', self._on_delete) if self._on_delete else (),
            (b'ON', b'UPDATE', self._on_update) if self._on_update else (),
        )
