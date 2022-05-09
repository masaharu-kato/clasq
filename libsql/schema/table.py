"""
    Table classes
"""
from typing import TYPE_CHECKING, Dict, Iterable, Iterator, List, Optional, Tuple, Type, Union

from ..syntax.keywords import ReferenceOption
from ..syntax.object_abc import FrozenObjectSet, OrderedFrozenObjectSet, to_name
from ..syntax.exprs import NamedExprABC, ObjectABC, Object, Name
from ..syntax.query_abc import iter_objects
from ..syntax.query_data import QueryData
from ..syntax import sqltypes
from ..syntax import errors
from ..utils.tabledata import TableData
from .column import Column
from .view import ViewABC, View

if TYPE_CHECKING:
    from .database import Database

class Table(ViewABC):
    """ Table Expr """

    def __init__(self,
        name: Name,
        *columns: Column,
        database: Optional['Database'] = None,
        primary_key: Optional[Tuple[Union[Name, Column], ...]] = None,
        unique: Optional[Tuple[Union[Name, Column], ...]] = None,
        refs: Optional[List['ForeignKeyReference']] = None,
        fetch_from_db: Optional[bool] = None,
        dynamic: bool = False,
        # **options
    ):
        super().__init__(name)
        
        self._database = database
        self._column_dict: Dict[bytes, Column] = {}
        self._dynamic = dynamic
        
        if fetch_from_db is True and columns:
            raise errors.ObjectArgsError('Columns are ignored when fetch_from_db is True')
        if fetch_from_db is not False and not columns: 
            self.fetch_from_db()
        else:
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

    @property
    def database_or_none(self) -> Optional['Database']:
        return self._database

    def iter_columns(self) -> Iterator[Column]:
        return (c for c in self._column_dict.values())

    @property
    def columns(self):
        return tuple(self.iter_columns())

    @property
    def available_named_exprs(self) -> FrozenObjectSet[NamedExprABC]:
        return self.columns

    @property
    def named_exprs(self) -> OrderedFrozenObjectSet[NamedExprABC]:
        return OrderedFrozenObjectSet(*self.available_named_exprs)

    @property
    def outer_named_exprs(self) -> OrderedFrozenObjectSet[NamedExprABC]:
        return self.named_exprs

    @property
    def base_view(self) -> 'ViewABC':
        return self
    
    @property
    def query_table_expr(self) -> QueryData:
        return QueryData(self)

    @property
    def query_table_expr_for_join(self) -> QueryData:
        return self.query_table_expr

    def set_database(self, database: 'Database') -> None:
        if self._database is not None:
            raise errors.ObjectAlreadySetError('Database is already set.')
        self._database = database
        
    @property
    def is_dynamic(self) -> bool:
        return self._dynamic

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

        raise errors.ObjectArgTypeError('Invalid type', val)

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

        if column.table_or_none is None:
            column.set_table(self)

        elif not column.table_or_none == self:
            raise errors.NotaSelfObjectError('Column of the different table.')
            
        if column.name in self._column_dict:
            raise errors.ObjectNameAlreadyExistsError('Column name already exists.', column)
            
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
        if column.name in self._column_dict:
            raise errors.ObjectNameAlreadyExistsError(column)
        self._column_dict[column.name] = column
        return column

    def fetch_from_db(self) -> None:
        """ Fetch columns of this table from the connected database """
        if self.database_or_none is None:
            raise errors.ObjectNotSetError('Database is not set.')
        self._column_dict.clear()
        for coldata in self.db.query(b'SHOW', b'COLUMNS', b'FROM', self.name):
            self.append_column(coldata['Field']) # TODO: Types, Nullable, Default, Keys
        # TODO: Primary, Unique, etc...
        self._exists_on_db = True

    def set_primary_key(self, *columns: Union[Name, Column]):
        self._primary_key = [self.table_column(c) for c in columns]
        
    def set_unique(self, *columns: Union[Name, Column]):
        self._unique = [self.table_column(c) for c in columns]

    @property
    def query_for_select_column(self) -> QueryData:
        return QueryData(self, b'.*')

    def select(self, *exprs, **options) -> TableData:
        """ Run SELECT query """
        return self.clone(*exprs, **options).result

    def insert(self, data, **values) -> int:
        """ Run INSERT query """
        return self.db.insert(self, data, **values)

    def update(self, data, **options) -> None:
        """ Run UPDATE query """
        self.db.update(self, data, **options)

    def delete(self, **options) -> None:
        """ Run DELETE query """
        self.db.delete(self, **options)

    def truncate(self) -> None:
        """ Run TRUNCATE TABLE query """
        self.db.execute(b'TRUNCATE', b'TABLE', self)

    def drop(self, *, temporary=False, if_exists=False) -> None:
        """ Run DROP TABLE query """
        if_exists = if_exists or (not self._exists_on_db)
        self.db.execute(
            b'DROP', b'TEMPORARY' if temporary else None, b'TABLE',
            (b'IF', b'EXISTS') if if_exists else None, self)
        self._exists_on_db = False
        self.db.remove_table(self)

    def create(self, *, temporary=False, if_not_exists=False, drop_if_exists=False) -> None:
        """ Create this Table on the database """
        if drop_if_exists:
            self.drop(temporary=temporary, if_exists=True)
        self.db.execute(
            b'CREATE', b'TEMPORARY' if temporary else None, b'TABLE',
            b'IF NOT EXISTS' if if_not_exists else None,
            self, b'(', [c.query_for_create_table for c in self.columns], b')'
        )
        self.fetch_from_db()
        
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

    def append_to_query_data(self, qd: QueryData) -> None:
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
            if e.table_or_none is not None:
                yield e.table
        elif isinstance(e, Table):
            yield e
