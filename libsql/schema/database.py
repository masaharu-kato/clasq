"""
    Database class definition
"""
from typing import TYPE_CHECKING, Collection, Dict, Iterable, Iterator, Optional, Tuple, Union, overload


from ..syntax.object_abc import NameLike, ObjectName
from ..syntax.exprs import Object
from ..syntax.query_data import QueryLike, QueryArgVals
from ..syntax.values import ValueType
from ..syntax.errors import NotaSelfObjectError, ObjectArgsError, ObjectNameAlreadyExistsError, ObjectNotFoundError, ObjectNotSetError
from ..utils.tabledata import TableData
from .column import ColumnArgs
from .table import Table, TableArgs
from .sqltypes import AnySQLType

if TYPE_CHECKING:
    from ..connection import ConnectionABC
    

class Database(Object):
    """ Database Expr """

    def __init__(self,
        name: NameLike,
        *table_args: TableArgs,
        con: Optional['ConnectionABC'] = None,
        charset: Optional[NameLike] = None,
        collate: Optional[NameLike] = None,
        fetch_from_db: Optional[bool] = None,
        # **options
    ):
        super().__init__(name)
        self._table_dict: Dict[ObjectName, Table] = {}
        self._con: Optional['ConnectionABC'] = None
        self._charset = ObjectName(charset) if charset is not None else None
        self._collate = ObjectName(collate) if collate is not None else None
        # self._options = options

        self.set_con(con)

        if fetch_from_db is True and table_args:
            raise ObjectArgsError('Tables are ignored when fetch_from_db is True')
        if self.connection_available and fetch_from_db is not False and not table_args: 
            self.fetch_from_db()
        else:
            for table_arg in table_args:
                self.append_table(table_arg)

    @property
    def con(self):
        if self._con is None:
            raise ObjectNotSetError('Connection is not set.')
        return self._con

    @property
    def connection(self):
        return self.con

    @property
    def connection_available(self) -> bool:
        return self._con is not None and self.con

    def set_con(self, con: Optional['ConnectionABC']) -> None:
        if con is None:
            if self._con is not None:
                self._con.set_db(None)
                self._con = None
        else:
            if self._con is None:
                con.set_db(self)
                self._con = con
            else:
                if self._con is not con:
                    raise RuntimeError('Connection is already set.')

    @property
    def charset(self):
        return self._charset

    @property
    def collate(self):
        return self._collate

    # @property
    # def options(self):
    #     return self._options

    @property
    def database(self) -> 'Database':
        return self

    @property
    def exists(self):
        return bool(self.con)

    def iter_tables(self):
        return iter(self._table_dict.values())

    @property
    def tables(self):
        return list(self.iter_tables())

    def table(self, val: NameLike) -> Table:
        """ Get a Table object with the specified name

        Args:
            val (bytes | str | ObjectName): Table name

        Raises:
            ObjectNotFoundError: Table not found.

        Returns:
            Table: Table object with the specified name
        """
        name = ObjectName(val)
        if name not in self._table_dict:
            raise ObjectNotFoundError('Table not found.', name)
        return self._table_dict[name]

    @overload
    def __getitem__(self, val: NameLike) -> Table: ...
    
    @overload
    def __getitem__(self, val: Tuple[NameLike, ...]) -> Tuple[Table, ...]: ...

    def __getitem__(self, val):
        if isinstance(val, tuple):
            return (*(self.table(v) for v in val),)
        return self.table(val)

    def table_or_none(self, val: NameLike) -> Optional[Table]:
        """ Get a Table object with the specified name if exists """
        try:
            return self.table(val)
        except ObjectNotFoundError:
            pass
        return None

    def get(self, val: NameLike) -> Optional[Table]:
        """ Synonym of `table_or_none` method """
        return self.table_or_none(val)

    def to_table(self, val: Union[NameLike, Table]) -> Table:
        if not isinstance(val, Table):
            return self.table(val)
        if val.database == self:
            return val
        raise NotaSelfObjectError('Not a table of this database.')

    def to_table_or_none(self, val: Union[NameLike, Table]) -> Optional[Table]:
        try:
            return self.to_table(val)
        except (ObjectNotFoundError, NotaSelfObjectError):
            pass
        return None

    def __contains__(self, val: Union[NameLike, Table])-> bool:
        return self.to_table_or_none(val) is not None

    @overload
    def append_table(self, name: NameLike, /, *column_args: ColumnArgs, **options) -> Table: ...

    @overload
    def append_table(self, table_arg: TableArgs, /) -> Table: ...

    def append_table(self, arg1, /, *cols, **kwargs) -> Table:
        """ Append a new table to this Database """

        if isinstance(arg1, TableArgs):
            table_arg = arg1
        else:
            table_arg = TableArgs(arg1, *cols, **kwargs)

        if table_arg.name in self._table_dict:
            raise ObjectNameAlreadyExistsError('Table name object already exists.', table_arg)

        table = Table(self, table_arg)
        self._table_dict[table.name] = table
        return self._table_dict[table.name]

    def remove_table(self, table: Table) -> None:
        table_name = self.to_table(table).name
        del self._table_dict[table_name]

    def fetch_from_db(self) -> None:
        """ Fetch tables of this database from the connection """
        for tabledata in self.query(b'SHOW', b'TABLES'):
            table_name = str(tabledata[0]).encode()
            self.append_table(TableArgs(table_name, *(
                ColumnArgs(coldata['Field'], AnySQLType)  # TODO: Fix type
                for coldata in self.query(b'SHOW', b'COLUMNS', b'FROM', table_name)
            )))

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
        if_exists = if_exists # or (not self.exists)
        self.execute(
            b'DROP', b'DATABASE',
            (b'IF', b'EXISTS') if if_exists else None, self)
        # self._exists = False

    def create(self, *, if_not_exists=False, drop_if_exists=False) -> None:
        """ Create this database """
        if drop_if_exists:
            self.drop(if_exists=True)
        self.execute(*self.q_create(if_not_exists=if_not_exists))
        # self._exists = True

    def query(self, *exprs: Optional[QueryLike], prms: Collection[ValueType] = ()) -> TableData:
        return self.con.query(*exprs, prms=prms)

    def query_many(self, *exprs: Optional[QueryLike], data: Union[TableData, Iterable[QueryArgVals]]) -> Iterator[TableData]:
        return self.con.query_many(*exprs, data=data)

    def execute(self, *exprs: Optional[QueryLike], prms: Collection[ValueType] = ()) -> None:
        return self.con.execute(*exprs, prms=prms)

    def execute_many(self, *exprs: Optional[QueryLike], data: Union[TableData, Iterable[QueryArgVals]]) -> None:
        return self.con.execute_many(*exprs, data=data)

    def commit(self) -> None:
        return self.con.commit()
