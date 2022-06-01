"""
    Database class definition
"""
from abc import abstractmethod, abstractproperty
from typing import TYPE_CHECKING, Collection, Dict, Iterable, Iterator, Optional, Tuple, Union, cast, overload


from ...syntax.abc.object import NameLike, ObjectABC, ObjectName
from ...syntax.exprs import Object
from ...syntax.query_data import QueryLike, QueryArgVals
from ...syntax.values import ValueType
from ...syntax.errors import NotaSelfObjectError, ObjectNameAlreadyExistsError, ObjectNotFoundError
from ...utils.tabledata import TableData
from ..column import ColumnArgs
from ..sqltypes import AnySQLType
from .table import TableABC, TableArgs

if TYPE_CHECKING:
    from ...connection import ConnectionABC
    

class DatabaseABC(ObjectABC):
    """ Database Expr """

    @abstractproperty
    def _con(self) -> 'ConnectionABC':
        """ Get a connection """
        raise NotImplementedError()

    @property
    def _connection(self) -> 'ConnectionABC':
        return self._con

    def get_connection(self) -> 'ConnectionABC':
        return self._con

    @abstractproperty
    def _connection_available(self) -> bool:
        """ Check if the connection is available """
        raise NotImplementedError()

    @abstractmethod
    def connect(self, con: Optional['ConnectionABC']) -> None:
        """ Set a new connection to this database """
        raise NotImplementedError()
        
    def disconnect(self) -> None:
        return self.connect(None)

    @abstractproperty
    def _charset(self):
        """ Get a charset for this database """
        raise NotImplementedError()

    @abstractproperty
    def _collate(self):
        """ Get a collation for this database """
        raise NotImplementedError()

    @property
    def _database(self) -> 'DatabaseABC':
        return self

    @abstractproperty
    def _table_dict(self) -> Dict[ObjectName, TableABC]:
        """ Get a table dict """
        raise NotImplementedError()

    def iter_tables(self):
        return iter(self._table_dict.values())

    @property
    def all_tables(self):
        return list(self.iter_tables())

    def get_table(self, val: NameLike) -> TableABC:
        """ Get a Table object with the specified name

        Args:
            val (bytes | str | ObjectName): Table name

        Raises:
            ObjectNotFoundError: Table not found.

        Returns:
            TableABC: Table object with the specified name
        """
        name = ObjectName(val)
        if name not in self._table_dict:
            raise ObjectNotFoundError('Table not found.', name)
        return self._table_dict[name]

    @overload
    def __getitem__(self, val: NameLike) -> TableABC: ...
    
    @overload
    def __getitem__(self, val: Tuple[NameLike, ...]) -> Tuple[TableABC, ...]: ...

    def __getitem__(self, val):
        if isinstance(val, tuple):
            return (*(self.get_table(v) for v in val),)
        return self.get_table(val)

    def get_table_or_none(self, val: NameLike) -> Optional[TableABC]:
        """ Get a Table object with the specified name if exists """
        try:
            return self.get_table(val)
        except ObjectNotFoundError:
            pass
        return None

    def _to_table(self, val: Union[NameLike, TableABC]) -> TableABC:

        try:
            is_table_abc = isinstance(val, TableABC)
        except TypeError:
            is_table_abc = False

        if is_table_abc:
            table = cast(TableABC, val)
            if table._database == self:
                return table
            raise NotaSelfObjectError('Not a table of this database.')
        
        return self.get_table(cast(NameLike, val))

    def _to_table_or_none(self, val: Union[NameLike, TableABC]) -> Optional[TableABC]:
        try:
            return self._to_table(val)
        except (ObjectNotFoundError, NotaSelfObjectError):
            pass
        return None

    def __contains__(self, val: Union[NameLike, TableABC])-> bool:
        return self._to_table_or_none(val) is not None

    @abstractmethod
    def _new_table(self, table_arg: TableArgs) -> TableABC:
        """ Make a new table """
        raise NotImplementedError()

    @overload
    def append_table(self, table_obj: TableABC, /) -> TableABC: ...

    @overload
    def append_table(self, name: NameLike, /, *column_args: ColumnArgs, **options) -> TableABC: ...

    @overload
    def append_table(self, table_arg: TableArgs, /) -> TableABC: ...

    def append_table(self, arg1, /, *cols, **kwargs) -> TableABC:
        """ Append a new table to this Database """

        try:
            is_table_abc = isinstance(arg1, TableABC)
        except TypeError:
            is_table_abc = False

        if is_table_abc:
            return self.append_table_object(cast(TableABC, arg1))
            
        if isinstance(arg1, TableArgs):
            table_arg = arg1
        else:
            table_arg = TableArgs(arg1, *cols, **kwargs)
        table = self._new_table(table_arg)

        table_name = table.get_name()
        if table_name in self._table_dict:
            raise ObjectNameAlreadyExistsError('Table name object already exists.', table_name)

        self._table_dict[table_name] = table
        return self._table_dict[table_name]

    def append_table_object(self, table: TableABC):
        if table._database is not self:
            raise NotaSelfObjectError('Not a table of this database.', table)
        table_name = table.get_name()
        if table_name in self._table_dict:
            raise ObjectNameAlreadyExistsError('Table name object already exists.', table)
        self._table_dict[table_name] = table
        return self._table_dict[table_name]

    def remove_table(self, table: TableABC) -> None:
        table_name = self._to_table(table).get_name()
        del self._table_dict[table_name]

    def fetch_from_db(self) -> None:
        """ Fetch tables of this database from the connection """
        for tabledata in self.query(b'SHOW', b'TABLES'):
            table_name = str(tabledata[0]).encode()
            self.append_table(TableArgs(table_name, *(
                ColumnArgs(coldata['Field'], AnySQLType)  # TODO: Fix type
                for coldata in self.query(b'SHOW', b'COLUMNS', b'FROM', table_name)
            )))

    def _create_database_query(self, *, if_not_exists=False) -> tuple:
        return (
            b'CREATE', b'DATABASE',
            (b'IF', b'NOT', b'EXISTS') if if_not_exists else None,
            self,
            (b'CHARACTER', b'SET', Object(self._charset)) if self._charset else None,
            (b'COLLATE', Object(self._collate)) if self._collate else None,
        )

    def drop(self, *, if_exists=False) -> None:
        """ Run DROP DATABASE query """
        if_exists = if_exists # or (not self.exists)
        self.execute(
            b'DROP', b'DATABASE',
            (b'IF', b'EXISTS') if if_exists else None, self)
        # self._exists = False

    def create(self, *, if_not_exists=False, drop_if_exists=False) -> None:
        """ Create this database """
        if drop_if_exists:
            self.drop(if_exists=True)
        self.execute(*self._create_database_query(if_not_exists=if_not_exists))
        # self._exists = True

    def query(self, *exprs: Optional[QueryLike], prms: Collection[ValueType] = ()) -> TableData:
        return self._con.query(*exprs, prms=prms)

    def query_many(self, *exprs: Optional[QueryLike], data: Union[TableData, Iterable[QueryArgVals]]) -> Iterator[TableData]:
        return self._con.query_many(*exprs, data=data)

    def execute(self, *exprs: Optional[QueryLike], prms: Collection[ValueType] = ()) -> None:
        return self._con.execute(*exprs, prms=prms)

    def execute_many(self, *exprs: Optional[QueryLike], data: Union[TableData, Iterable[QueryArgVals]]) -> None:
        return self._con.execute_many(*exprs, data=data)

    def commit(self) -> None:
        return self._con.commit()



class DatabaseReferenceABC(DatabaseABC):
    """ Database Reference abstract class """

    @abstractmethod
    def get_entity(cls) -> DatabaseABC:
        """ Get a database object """
        raise NotImplementedError()

    def get_name(self):
        return self._entity.get_name()
    
    @property
    def _entity(self) -> DatabaseABC:
        return self.get_entity()

    @property
    def _con(self):
        return self._entity._con

    @property
    def _connection_available(self) -> bool:
        return self._entity._connection_available

    def connect(self, con: Optional['ConnectionABC']) -> None:
        return self._entity.connect(con)

    @property
    def _charset(self):
        return self._entity._charset

    @property
    def _collate(self):
        return self._entity._collate

    @property
    def _table_dict(self):
        return self._entity._table_dict
