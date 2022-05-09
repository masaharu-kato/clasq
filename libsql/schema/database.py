"""
    Database class definition
"""
from typing import TYPE_CHECKING, Collection, Dict, Iterable, Iterator, Optional, Tuple, Union, overload

from ..syntax.exprs import Object, Name
from ..syntax.query_data import QueryLike, QueryArgVals
from ..syntax.values import ValueType
from ..syntax import errors
from ..utils.tabledata import TableData
from .table import Table

if TYPE_CHECKING:
    from ..connection import ConnectionABC
    

class Database(Object):
    """ Database Expr """

    def __init__(self,
        name: Name,
        *tables: Table,
        cnx: Optional['ConnectionABC'] = None,
        charset: Optional[Name] = None,
        collate: Optional[Name] = None,
        fetch_from_db: Optional[bool] = None,
        dynamic: bool = False,
        **options
    ):
        super().__init__(name)
        self._table_dict: Dict[bytes, Table] = {}
        self._cnx = cnx
        self._charset = charset
        self._collate = collate
        self._exists = bool(cnx)
        self._dynamic = dynamic
        self._options = options


        if fetch_from_db is True and tables:
            raise errors.ObjectArgsError('Tables are ignored when fetch_from_db is True')
        if fetch_from_db is not False and not tables: 
            self.fetch_from_db()
        else:
            for table in tables:
                self.append_table_object(table)

    @property
    def cnx(self):
        if self._cnx is None:
            raise errors.ObjectNotSetError('Connection is not set.')
        return self._cnx

    @property
    def charset(self):
        return self._charset

    @property
    def collate(self):
        return self._collate

    @property
    def options(self):
        return self._options

    @property
    def database(self) -> 'Database':
        return self

    @property
    def exists(self):
        return self._exists

    @property
    def is_dynamic(self):
        return self._dynamic

    def __repr__(self):
        return 'DB(%s)' % str(self)

    def iter_tables(self):
        return iter(self._table_dict.values())

    @property
    def tables(self):
        return list(self.iter_tables())

    def table(self, val: Union[Name, Table]) -> Table:
        """ Get a Table object with the specified name

        Args:
            val (bytes | str | Table): Table name or table object

        Raises:
            errors.ObjectNotFoundError: _description_
            errors.NotaSelfObjectError: _description_
            errors.ObjectArgsError: _description_

        Returns:
            Table: Table object with the specified name or Table object itself
        """
        if isinstance(val, (bytes, str)):
            name = val.encode() if isinstance(val, str) else val
            if name not in self._table_dict:
                if self.is_dynamic:
                    return self.append_table(name, dynamic=True)
                else:
                    raise errors.ObjectNotFoundError('Undefined table name `%s` on database `%s`' % (str(name), str(self._name)))
            return self._table_dict[name] 

        if isinstance(val, Table):
            if val.database == self:
                return val
            raise errors.NotaSelfObjectError('Not a table of this database.')

        raise errors.ObjectArgsError('Invalid type %s (%s)' % (type(val), val))
        
    @overload
    def __getitem__(self, val: Union[Name, Table]) -> Table: ...
    
    @overload
    def __getitem__(self, val: Tuple[Union[Name, Table], ...]) -> Tuple[Table, ...]: ...

    def __getitem__(self, val):
        if isinstance(val, tuple):
            return (*(self.table(v) for v in val),)
        return self.table(val)

    def table_or_none(self, val: Union[Name, Table]) -> Optional[Table]:
        """ Get a Table object with the specified name if exists """
        try:
            return self.table(val)
        except (errors.ObjectNotFoundError, errors.NotaSelfObjectError):
            pass
        return None

    def get(self, val: Union[Name, Table]) -> Optional[Table]:
        """ Synonym of `table_or_none` method """
        return self.table_or_none(val)

    def append_table_object(self, table: Table) -> None:
        """ Append (existing) Table object to this Database

        Args:
            table (Table): Table object

        Raises:
            errors.NotaSelfObjectError: The Table object in the different database was specified
        """
        if table.database_or_none is not None:
            if not table.database == self:
                raise errors.NotaSelfObjectError('Table of the different database.')
        else:
            table.set_database(self)

        if table.name in self._table_dict:
            raise errors.ObjectNameAlreadyExistsError('Table name object already exists.', table)

        self._table_dict[table.name] = table

    def append_table(self, name: Name, *, fetch=False, **options) -> 'Table':
        """ Append new table to this Database

        Args:
            name (bytes | str): Table name
            options: Table options

        Returns:
            Table: Appended new Table object
        """
        self.append_table_object(Table(name, database=self, fetch_from_db=fetch, **options))
        return self.table(name)

    def remove_table(self, table: Table) -> None:
        table_name = self.table(table).name
        del self._table_dict[table_name]

    def fetch_from_db(self) -> None:
        """ Fetch tables of this database from the connection """
        for tabledata in self.query(b'SHOW', b'TABLES'):
            self.append_table(tabledata[0], fetch=True)

    def q_create(self, *, if_not_exists=False) -> tuple:
        return (
            b'CREATE', b'DATABASE',
            (b'IF', b'NOT', b'EXISTS') if if_not_exists else None,
            self,
            (b'CHARACTER', b'SET', Object(self._charset)) if self._charset else None,
            (b'COLLATE', Object(self._collate)) if self._collate else None,
        )

    def drop(self, *, if_exists=False) -> None:
        """ Run DROP DATABASE query """
        if_exists = if_exists or (not self._exists)
        self.execute(
            b'DROP', b'DATABASE',
            (b'IF', b'EXISTS') if if_exists else None, self)
        self._exists = False

    def create(self, *, if_not_exists=False, drop_if_exists=False) -> None:
        """ Create this database """
        if drop_if_exists:
            self.drop(if_exists=True)
        self.execute(*self.q_create(if_not_exists=if_not_exists))
        self._exists = True

    def query(self, *exprs: Optional[QueryLike], prms: Collection[ValueType] = ()) -> TableData:
        return self.cnx.query(*exprs, prms=prms)

    def query_many(self, *exprs: Optional[QueryLike], data: Union[TableData, Iterable[QueryArgVals]]) -> Iterator[TableData]:
        return self.cnx.query_many(*exprs, data=data)

    def execute(self, *exprs: Optional[QueryLike], prms: Collection[ValueType] = ()) -> None:
        return self.cnx.execute(*exprs, prms=prms)

    def execute_many(self, *exprs: Optional[QueryLike], data: Union[TableData, Iterable[QueryArgVals]]) -> None:
        return self.cnx.execute_many(*exprs, data=data)

    def commit(self) -> None:
        return self.cnx.commit()
