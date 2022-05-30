"""
    Database data class
"""

from typing import Optional, TYPE_CHECKING
from ..schema.database import Database
from ..utils.name_conversion import camel_to_snake

if TYPE_CHECKING:
    from ..connection.connection import ConnectionABC
    from ..syntax.object_abc import NameLike


class DatabaseClass:
    """ Database class """

    _db_obj : 'Database'
    # _instance: Optional['DatabaseClass'] = None

    @classmethod
    def _db_name(cls) -> 'NameLike':
        if cls is DatabaseClass:
            raise RuntimeError('DatabaseClass is not specialized.')
        return camel_to_snake(cls.__name__)

    @classmethod
    def _db_charset(cls) -> Optional['NameLike']:
        return None

    @classmethod
    def _db_collate(cls) -> Optional['NameLike']:
        return None

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls._db_obj = Database(name=cls._db_name(), charset=cls._db_charset(), collate=cls._db_collate())

    # @classmethod
    # def get_schema_object(cls) -> 'Database':
    #     """ Get a schema object of this database """
    #     # TODO: Append table
    
    def __new__(cls, *args, **kwargs):
        raise RuntimeError('Cannot instantiate a DatabaseClass.')

    # def __new__(cls, *args, **kwargs):
    #     if cls._instance is not None:
    #         raise RuntimeError('Cannot create multiple database objects.')
    #     cls._instance = super().__new__()
    #     return cls._instance

    # def __init__(self, con: 'ConnectionABC') -> None:
    #     super().__init__()
    #     if self.__class__ is DatabaseClass:
    #         raise RuntimeError('Cannot instantiate a DatabaseClass directly.')
    #     self._db_obj.set_con(con)
    