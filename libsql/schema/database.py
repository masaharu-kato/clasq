"""
    Database class definition
"""
from typing import TYPE_CHECKING, Dict, Optional, Tuple, overload


from ..syntax.object_abc import NameLike, ObjectName
from ..syntax.exprs import Object
from ..syntax.errors import ObjectArgsError, ObjectNotSetError
from .table_abc import TableABC, TableArgs
from .table import Table
from .database_abc import DatabaseABC

if TYPE_CHECKING:
    from ..connection import ConnectionABC
    

class Database(DatabaseABC, Object):
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
        self.__table_dict: Dict[ObjectName, TableABC] = {}
        self.__con: Optional['ConnectionABC'] = None
        self.__charset = ObjectName(charset) if charset is not None else None
        self.__collate = ObjectName(collate) if collate is not None else None
        # self._options = options

        self.connect(con)

        if fetch_from_db is True and table_args:
            raise ObjectArgsError('Tables are ignored when fetch_from_db is True')
        if self._connection_available and fetch_from_db is not False and not table_args: 
            self.fetch_from_db()
        else:
            for table_arg in table_args:
                self.append_table(table_arg)

    @property
    def _con(self):
        """ Override for `DatabaseABC` """
        if self.__con is None:
            raise ObjectNotSetError('Connection is not set.')
        return self.__con

    @property
    def _connection_available(self) -> bool:
        """ Override for `DatabaseABC` """
        return self.__con is not None and self._con

    def connect(self, con: Optional['ConnectionABC']) -> None:
        """ Override for `DatabaseABC` """
        if con is None:
            if self.__con is not None:
                self.__con.set_db(None)
                self.__con = None
        else:
            if self.__con is None:
                con.set_db(self)
                self.__con = con
            else:
                if self.__con is not con:
                    raise RuntimeError('Connection is already set.')

    @property
    def _charset(self):
        return self.__charset

    @property
    def _collate(self):
        return self.__collate

    @property
    def _table_dict(self) -> Dict[ObjectName, TableABC]:
        return self.__table_dict

    def _new_table(self, table_arg: TableArgs) -> TableABC:
        """ Make a new table """
        return Table(self, table_arg)
