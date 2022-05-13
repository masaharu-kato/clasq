"""
    Column classes
"""
from abc import abstractproperty
from typing import TYPE_CHECKING, Optional, Union

from ..syntax.object_abc import NameLike, Object, ObjectABC, ObjectName
from ..syntax.exprs import ExprABC, ExprObjectABC, FrozenOrderedExprObjectSet
from ..syntax.keywords import OrderType, ReferenceOption
from ..syntax.query_abc import iter_objects
from ..syntax.query_data import QueryData, QueryLike
from ..syntax.errors import ObjectNameAlreadyExistsError, ObjectNotSetError
from ..syntax.sqltypes import SQLType, make_sql_type, get_type_sql
from ..utils.keyset import FrozenKeySetABC, FrozenOrderedKeySetABC

if TYPE_CHECKING:
    from .view import BaseViewABC, NamedViewABC
    from .table import Table, ForeignKeyReference
    from .database import Database


class ColumnABC(ExprObjectABC):
    """ Column ABC """

    @property
    def database(self):
        """ Get a parent Database object """
        return self.base_view.database

    @property
    def cnx(self):
        """ Get a database server connection """
        return self.base_view.cnx

        
class NamedViewColumnABC(ColumnABC, ObjectABC):
    """ Column object which belonging to the BaseView object """

    @abstractproperty
    def named_view(self) -> 'NamedViewABC':
        """ Get a belonging BaseView object 
        """

    @property
    def name_with_view(self) -> ObjectName:
        return self.named_view.name + self.name

    def append_to_query_data(self, qd: QueryData) -> None:
        """ Append this expression to the QueryData object

        Args:
            qd (QueryData): QueryData object to be appended
        """
        qd.append(self.named_view, b'.', self.name)


class NamedViewColumn(NamedViewColumnABC, Object):
    """ View Column expression """

    def __init__(self, named_view: 'NamedViewABC', name: NameLike) -> None:
        super().__init__(name)
        self._named_view = named_view

    @property
    def named_view(self) -> 'NamedViewABC':
        """ Get a belonging NamedView object 
            (Overridef from `NamedViewColumnABC`
        """
        return self._named_view

    @property
    def select_column_query(self) -> 'QueryLike':
        """ Get a query for SELECT column """
        return self

    def __repr__(self):
        return ('NmViCol[%s.%s](%s)'
            % (repr(self.base_view), self.name, repr(self.expr)))


class FrozenOrderedNamedViewColumnSet(FrozenOrderedKeySetABC[ObjectName, NamedViewColumnABC]):

    def _key(self, obj: NamedViewColumnABC) -> ObjectName:
        return obj.name_with_view

    def _key_or_none(self, obj) -> Optional[ObjectName]:
        return self._key(obj) if isinstance(obj, NamedViewColumnABC) else None


class TableColumnRef:
    def __init__(self,
        database: 'Database',
        table_like: Union[NameLike, 'Table'],
        column_name: NameLike
    ) -> None:
        self.database = database
        self.table_like = table_like
        self.column_name = column_name

    def resolve(self) -> 'TableColumn':
        return self.database.to_table(self.table_like).table_column(self.column_name)


class ColumnArgs:
    def __init__(self,
        name: NameLike,
        sql_type: Optional[SQLType] = None,
        *,
        not_null: bool = False,
        default = None, 
        unique: bool = False,
        primary: bool = False,
        auto_increment: bool = False,
        ref_column: Optional[Union[TableColumnRef, 'TableColumn']] = None,
        ref_on_delete: Optional[ReferenceOption] = None,
        ref_on_update: Optional[ReferenceOption] = None,
        ref_index_name: Optional[NameLike] = None
    ):
        self.name = name
        self.sql_type = sql_type
        self.not_null = not_null
        self.default = default
        self.unique = unique
        self.primary = primary
        self.auto_increment = auto_increment
        self.ref_column = ref_column
        self.ref_on_delete = ref_on_delete
        self.ref_on_update = ref_on_update
        self.ref_index_name = ref_index_name


class TableColumn(NamedViewColumnABC, Object):
    """ Table Column expression """

    def __init__(self, table: 'Table', args: ColumnArgs):
        self._table = table
        super().__init__(args.name)

        self._sql_type = make_sql_type(args.sql_type) if args.sql_type is not None else None
        self._not_null = args.not_null or args.primary
        self._table = table
        self._default_value = args.default
        self._is_unique = args.unique
        self._is_primary = args.primary
        self._is_auto_increment = args.auto_increment
        self._reference: Optional['ForeignKeyReference'] =  None

        if args.ref_column is not None:
            self._reference = ForeignKeyReference(
                self,
                args.ref_column.resolve() if isinstance(args.ref_column, TableColumnRef) else args.ref_column,
                on_update=args.ref_on_update,
                on_delete=args.ref_on_delete,
                name=args.ref_index_name,
            )

    @property
    def named_view(self) -> 'NamedViewABC':
        """ Get a belonging BaseView object 
            (Overridef from `NamedViewColumnABC`
        """
        return self._table

    @property
    def sql_type(self) -> Optional[SQLType]:
        return self._sql_type

    @property
    def is_not_null_type(self) -> bool:
        return self._not_null

    @property
    def order_type(self) -> OrderType:
        return OrderType.ASC

    @property
    def original_column(self) -> ExprABC:
        return self

    @property
    def base_view(self) -> 'BaseViewABC':
        return self._table

    @property
    def table_or_none(self):
        return self._table

    @property
    def table(self) -> 'Table':
        if self._table is None:
            raise ObjectNotSetError('Table is not set.')
        return self._table

    @property
    def default_value(self):
        return self._default_value

    # @property
    # def comment(self):
    #     return self._comment

    @property
    def is_unique(self):
        return self._is_unique

    @property
    def is_primary(self):
        return self._is_primary

    @property
    def is_auto_increment(self):
        return self._is_auto_increment

    @property
    def query_for_create_table(self) -> QueryData:
        return QueryData(
            self.name, b' ',
            get_type_sql(self.sql_type).encode(),
            (b'NOT', b'NULL') if self.is_not_null_type else None,
            (b'DEFAULT', self.default_value) if self.default_value else None,
            b'AUTO_INCREMENT' if self.is_auto_increment else None,
            b'UNIQUE' if self.is_unique else None,
            (b'PRIMARY', b'KEY') if self.is_primary else None,
            self._reference,
        )

    def __repr__(self):
        return 'TC(%s->%s)' % (repr(self.base_view), self.name)


def iter_columns(*exprs: Optional[ObjectABC]):
    for e in iter_objects(*exprs):
        if isinstance(e, TableColumn):
            yield e
