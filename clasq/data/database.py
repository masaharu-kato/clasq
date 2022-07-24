"""
    Database data class
"""
from __future__ import annotations
from typing import TYPE_CHECKING, get_type_hints

from ..schema.abc.database import DatabaseReferenceABC
from ..schema.database import Database
from ..schema.abc.table import TableArgs
from ..schema.table import Table
from ..utils.name_conversion import camel_to_snake

from .table_record import TableClass

if TYPE_CHECKING:
    from ..syntax.abc.object import NameLike


class _DatabaseClassMeta(type, DatabaseReferenceABC):
    """ Database Metaclass """
    

class DatabaseClass(metaclass=_DatabaseClassMeta):
    """ Database class """

    _db_name: str | None = None
    _db_charset: str | None = None
    _db_collate: str | None = None
    __db_obj: Database
    # _instance: DatabaseClass | None = None

    @classmethod
    def get_entity(cls) -> Database:
        return cls.__db_obj

    @classmethod
    def _get_db_name(cls) -> NameLike:
        if cls is DatabaseClass:
            raise RuntimeError('DatabaseClass is not specialized.')
        if cls._db_name:
            return cls._db_name
        return camel_to_snake(cls.__name__)

    @classmethod
    def _get_db_charset(cls) -> NameLike | None:
        return cls._db_charset

    @classmethod
    def _get_db_collate(cls) -> NameLike | None:
        return cls._db_collate

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls.__db_obj = Database(name=cls._get_db_name(), charset=cls._get_db_charset(), collate=cls._get_db_collate())

        for hint_name, type_hint in get_type_hints(cls).items():
            if hint_name[0:1] == '_':
                continue

            if not (isinstance(type_hint, type) and issubclass(type_hint, TableClass)):
                raise TypeError('Invalid table object type.')
                
            col_args = type_hint._read_column_args()
            type_hint._set_table_object(Table(cls.__db_obj, TableArgs(hint_name, col_args)))


    # @classmethod
    # def get_schema_object(cls) -> Database:
    #     """ Get a schema object of this database """
    #     # TODO: Append table
    
    def __new__(cls, *args, **kwargs):
        raise RuntimeError('Cannot instantiate a DatabaseClass.')

    # def __new__(cls, *args, **kwargs):
    #     if cls._instance is not None:
    #         raise RuntimeError('Cannot create multiple database objects.')
    #     cls._instance = super().__new__()
    #     return cls._instance

    # def __init__(self, con: ConnectionABC) -> None:
    #     super().__init__()
    #     if self.__class__ is DatabaseClass:
    #         raise RuntimeError('Cannot instantiate a DatabaseClass directly.')
    #     self._db_obj.set_con(con)
    