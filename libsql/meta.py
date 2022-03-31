"""
    Meta classes
"""
from abc import abstractmethod
from ast import List, Tuple
from libsql.syntax.sql_expression import SQLExprType


class SQLObjABC(SQLExprType):
    """ SQL Schema object abstract class """

    @abstractmethod
    def __hash__(self) -> int:
        """ Get the hash value """
    
    @abstractmethod
    def sql(self) -> str:
        """ Get SQL expression of this schema object """

    def sql_with_params(self) -> Tuple[str, List]:
        """ Output sql and its placeholder parameters """
        return self.sql(), []


def tosql(obj) -> str:
    if isinstance(obj, SQLObjABC):
        return obj.sql()
    if isinstance(obj, str): # ColumnAlias
        return str(obj)
    raise TypeError('Invalid type of SQL object.')

