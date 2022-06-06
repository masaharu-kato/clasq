"""
    Column classes
"""
from __future__ import annotations
from typing import TYPE_CHECKING, Generic, TypeVar

from ..syntax.abc.object import NameLike, Object, ObjectABC, ObjectName
from ..syntax.abc.query import iter_objects
from ..syntax.query import QueryLike
from ..syntax.abc.data_types import DataTypeABC
from ..syntax.data_types import Nullable
from ..syntax.sql_parse import data_type_from_sql
from ..utils.keyset import FrozenOrderedKeySetABC
from ..errors import ObjectNotFoundError, ReferenceResolveError
from .abc.column import NamedViewColumnABC, TableColumnABC, TableColumnReferenceABC, TypedNamedViewColumnABC, TypedTableColumnABC

if TYPE_CHECKING:
    from .abc.view import NamedViewABC
    from .abc.table import TableABC


DT = TypeVar('DT', bound=DataTypeABC)
class NamedViewColumn(TypedNamedViewColumnABC[DT], Object, Generic[DT]):
    """ View Column expression """

    def __init__(self, named_view: NamedViewABC, name: NameLike) -> None:
        super().__init__(name)
        self.__named_view = named_view

    @property
    def view(self) -> NamedViewABC:
        """ Get a belonging NamedView object 
            (Overridef from `NamedViewColumnABC`)
        """
        return self.__named_view

    @property
    def _select_column_query(self) -> QueryLike:
        """ Get a query for SELECT column """
        return self

    def __repr__(self):
        return ('NmViCol[%s.%s](%s)'
            % (repr(self.base_view), self.name, repr(self.expr)))


class FrozenOrderedNamedViewColumnSet(FrozenOrderedKeySetABC[ObjectName, NamedViewColumnABC]):

    def _key(self, obj: NamedViewColumnABC) -> ObjectName:
        return obj._name_with_view

    def _key_or_none(self, obj) -> ObjectName | None:
        return self._key(obj) if isinstance(obj, NamedViewColumnABC) else None


class TableColumn(TypedTableColumnABC[DT], Object, Generic[DT]):
    """ Table Column expression """

    def __init__(self, table: TableABC, name: NameLike, *, default: DataTypeABC | None):
        self.__table = table
        super().__init__(name)
        self.__default_value = default

    @property
    def table(self):
        """ Get a parent Table object
            (Override for `TableColumnABC`)
        """
        return self.__table
        
    @property
    def default_value(self):
        """ Get a parent Table object
            (Override for `TableColumnABC`)
        """
        return self.__default_value

    def __repr__(self):
        return 'TC(%s->%s)' % (repr(self.table), self.name)


class UniqueTableColumn(TableColumn[DT], Generic[DT]):
    def is_unique(self):
        return True
        
class PrimaryTableColumn(UniqueTableColumn[DT], Generic[DT]):
    def is_primary(self):
        return True
        
class AutoIncrementPrimaryTableColumn(PrimaryTableColumn[DT], Generic[DT]):
    def is_auto_increment(self):
        return True


class TableColumnReference(TableColumnReferenceABC):
    """ Reference of Table Column """
    def __init__(self, table: TableABC, column_name: NameLike) -> None:
        super().__init__()
        self.__table = table
        self.__column_name = ObjectName(column_name)
        self.__dest_column: TableColumnABC | None = None

    @property
    def table_or_none(self) -> TableABC | None:
        return self.__table

    @property
    def _name(self) -> ObjectName:
        return self.__column_name

    def _dest_table_column(self) -> TableColumnABC:
        """ Get a destination Table Column 
            (Override for `TableColumnReferenceABC`)
        """
        if self.__dest_column is None:
            try:
                self.__dest_column = self.__table.get_table_column(self.__column_name)
            except ObjectNotFoundError as e:
                raise ReferenceResolveError(
                    'Failed to resolve reference.', self.__table, self.__column_name) from e
        return self.__dest_column


def make_table_column_type(
    data_type_sql: str, *,
    nullable: bool,
    is_unique: bool,
    is_primary: bool,
    is_auto_increment: bool,
) -> type[TableColumnABC]:
    data_type = data_type_from_sql(data_type_sql)
    if nullable:
        data_type = Nullable[data_type]  # type: ignore
    if is_unique or is_primary:
        if is_primary:
            if is_auto_increment:
                return AutoIncrementPrimaryTableColumn[data_type]  # type: ignore
            return PrimaryTableColumn[data_type]  # type: ignore
        return UniqueTableColumn[data_type]  # type: ignore
    return TableColumn[data_type]  # type: ignore


def iter_columns(*exprs: ObjectABC | None):
    for e in iter_objects(*exprs):
        if isinstance(e, TableColumn):
            yield e
