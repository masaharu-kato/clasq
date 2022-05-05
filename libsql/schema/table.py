"""
    Table classes
"""
from typing import TYPE_CHECKING, Dict, Iterable, Iterator, List, Optional, Tuple, Type, Union

from ..syntax.keywords import ReferenceOption
from ..syntax.exprs import NamedExprABC, ObjectABC, Object, Name, to_name, iter_objects
from ..syntax import sqltypes
from ..syntax import errors
from ..utils.tabledata import TableData
from .column import Column, NamedExpr
from .view import ViewABC, View

if TYPE_CHECKING:
    from ..syntax.query_data import QueryData
    from .database import Database

class Table(ViewABC):
    """ Table Expr """

    def __init__(self,
        name: Name,
        *_columns: Optional[Column],
        database: Optional['Database'] = None,
        primary_key: Optional[Tuple[Union[Name, Column], ...]] = None,
        unique: Optional[Tuple[Union[Name, Column], ...]] = None,
        refs: Optional[List['ForeignKeyReference']] = None,
        dynamic: bool = False,
        # **options
    ):
        super().__init__(name, *_columns, database=database, dynamic=dynamic)
        
        columns = [c for c in _columns if c is not None]
        self._column_dict: Dict[bytes, Column] = {}
        for column in columns:
            self.append_column_object(column)
        # self._options = options
        
        self._primary_key: Optional[List[Column]] = None
        self._unique: Optional[List[Column]] = None
        self._refs = refs

        if primary_key:
            self.set_primary_key(*primary_key)
        if unique:
            self.set_unique(*unique)


    def table_column(self, val: Union[Name, Column]) -> Column:
        """ Get a column from this table

        Args:
            val (bytes | str | ObjectABC): The column name or the column object

        Raises:
            ObjectNotFoundError: The column with the specified name cannot be found on this table.
            NotaSelfObjectError: The specified column object is not a column of this table.
            ObjectArgumentsTypeError: The specified value is not a valid type.

        Returns:
            ObjectABC: The column object
        """
        if isinstance(val, (bytes, str)):
            name = to_name(val)
            if name not in self._column_dict:
                if not self.is_dynamic:
                    raise errors.ObjectNotFoundError('Undefined column name `%r` on view `%r`' % (name, self._name))
                self._column_dict[name] = Column(name, table=self)
            return self._column_dict[name]
            
        if isinstance(val, Column):
            if not val.table_or_none == self:
                if not self.is_dynamic:
                    raise errors.NotaSelfObjectError('Not a column of this table.')
                self.append_column_object(val)
            return val

        raise errors.ObjectArgTypeError('Invalid type %s (%s)' % (type(val), val))

    def column(self, val: Union[Name, NamedExprABC]) -> NamedExprABC:
        if isinstance(val, NamedExprABC) and not isinstance(val, Column):
            raise errors.ObjectArgTypeError('Cannot specify non-Column object on Table class.')
        return self.table_column(val)

    def append_column_object(self, column: Column) -> None:
        """ Append (existing) Column object

        Args:
            column (Column): Column object

        Raises:
            errors.ObjectArgTypeError: _description_
            errors.NotaSelfObjectError: _description_
        """
        if not isinstance(column, Column):
            raise errors.ObjectArgTypeError('Invalid argument type %s (%s)' % (type(column), column))

        if not column.table_or_none:
            column.set_table(self)

        elif not column.table_or_none == self:
            raise errors.NotaSelfObjectError('Column of the different table.')
            
        self._column_dict[column.name] = column
        
    def append_column(self, 
        name: Name,
        typelike: sqltypes.TypeLike = None,
        *,
        not_null: bool = False,
        default = None,
        unique: bool = False,
        primary: bool = False,
        auto_increment: bool = False,
    ) -> 'Column':
        column = Column(
            name,
            sql_type=sqltypes.make_sql_type(typelike) if typelike is not None else None,
            table=self,
            default=default,
            not_null=not_null,
            unique=unique,
            primary=primary,
            auto_increment=auto_increment,
        )
        self._column_dict[column.name] = column
        return column

    def iter_columns(self) -> Iterator[Column]:
        return (c for c in self._column_dict.values())

    def set_primary_key(self, *columns: Union[Name, Column]):
        self._primary_key = [self.table_column(c) for c in columns]
        
    def set_unique(self, *columns: Union[Name, Column]):
        self._unique = [self.table_column(c) for c in columns]

    def query_for_select_column(self) -> tuple:
        return (self, b'.*')

    def select(self, *exprs, **options) -> TableData:
        """ Run SELECT query """
        return self.db.select(self, *exprs, **options)

    def insert(self, data, **values) -> int:
        """ Run INSERT query """
        return self.db.insert(self, data, **values)

    def update(self, data, **options):
        """ Run UPDATE query """
        return self.db.update(self, data, **options)

    def delete(self, **options):
        """ Run DELETE query """
        return self.db.delete(self, **options)

    def truncate(self):
        """ Run TRUNCATE TABLE query """
        return self.db.execute(b'TRUNCATE', b'TABLE', self)

    def drop(self, *, temporary=False, if_exists=False):
        """ Run DROP TABLE query """
        if_exists = if_exists or (not self._exists_on_db)
        return self.db.execute(
            b'DROP', b'TEMPORARY' if temporary else None, b'TABLE',
            (b'IF', b'EXISTS') if if_exists else None, self)

    def create(self, *, temporary=False, if_not_exists=False, drop_if_exists=False) -> None:
        """ Create this Table on the database """
        if drop_if_exists:
            self.drop(temporary=temporary, if_exists=True)
        self.db.execute(
            b'CREATE', b'TEMPORARY' if temporary else None, b'TABLE',
            b'IF NOT EXISTS' if if_not_exists else None,
            self, b'(', [c.query_for_create_table() for c in self.iter_columns()], b')'
        )
        self._exists_on_db = True

    def get_froms(self):
        return (self,)
        
    def _new_view(self, *args, **kwargs) -> 'ViewABC':
        return View(*args, **kwargs)

    def __repr__(self):
        return 'Table(%s)' % str(self)


class ForeignKeyReference(Object):
    """ Foreign Key Reference """

    def __init__(self,
        orig_column: Union[Column, Tuple[Column, ...]],
        ref_column : Union[Column, Tuple[Column, ...]],
        *,
        on_delete: Optional[ReferenceOption] = None,
        on_update: Optional[ReferenceOption] = None,
        name: Optional[Name] = None
    ):
        super().__init__(name or b'')
        
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

    def append_query_data(self, qd: 'QueryData') -> None:
        """ Append this to query data"""
        qd.append(
            b'FOREIGN', b'KEY', self.name, b'(', [super(Object, c) for c in self._orig_columns], b')',
            b'REFERENCES', self._ref_table, b'(', [super(Object, c) for c in self._ref_columns], b')',
            (b'ON', b'DELETE', self._on_delete) if self._on_delete else None,
            (b'ON', b'UPDATE', self._on_update) if self._on_update else None,
        )


def iter_tables(*exprs: Optional[ObjectABC]):
    for e in iter_objects(*exprs):
        if isinstance(e, Column):
            if e.table_or_none:
                yield e.table
        elif isinstance(e, Table):
            yield e
