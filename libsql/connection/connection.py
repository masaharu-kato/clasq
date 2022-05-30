"""
    SQL Connection classes and functions
"""
from abc import abstractmethod
from typing import TYPE_CHECKING, Collection, Iterable, Iterator, Optional, Type, Union


from ..syntax.object_abc import NameLike
from ..syntax.sql_values import SQLValue
from ..utils.tabledata import TableData
from ..schema.database import Database
from ..syntax.query_data import QueryData, QueryLike, ValueType, QueryArgVals
from . import errors


class ConnectionABC:
    """ Database connection ABC """

    def __init__(self, database: Optional[Union[str, Database]] = None,
                #  database_class: Optional[Type['DatabaseClass']] = None,
                 init_db=True, **other_cnx_options) -> None:
        """ Init """
        self._cnx_options = {**other_cnx_options}
        self._db: Optional[Database] = None

        if isinstance(database, Database):
            self._cnx_options['database'] = database.raw_name
            self._db = database

        elif isinstance(database, str):
            self._cnx_options['database'] = database
            self._db = Database(database)
            self._db.connect(self)
            self._db.fetch_from_db()

    @property
    def db(self) -> Database:
        if self._db is None:
            raise RuntimeError('Database is not initialized.')
        return self._db

    def set_db(self, db: Optional['Database']) -> None:
        if db is None:
            if self._db is not None:
                self._db = None
        else:
            if self._db is None:
                self._db = db
                self._use_db(db.get_name())
            else:
                if self._db is not db:
                    raise RuntimeError('Database is already set.')
    
    @property
    def cnx_options(self):
        return self._cnx_options

    @abstractmethod
    def _use_db(self, dbname: NameLike) -> None:
        """ Execute a USE database query """

    @abstractmethod
    def run_stmt_prms(self, stmt: bytes, prms: Collection[SQLValue] = ()) -> Optional[TableData]:
        """ Execute a query with single list of params and get result if exists """

    @abstractmethod
    def run_stmt_many_prms(self, stmt: bytes, prms_list: Iterable[Collection[SQLValue]]) -> Iterator[Optional[TableData]]:
        """ Execute a query with multiple lists of params and get result if exists """

    def query(self, *exprs: Optional[QueryLike], prms: Collection[ValueType] = ()) -> TableData:
        if (result := self.run(*exprs, prms)) is None:
            raise errors.NoResultsError('No results.')
        return result

    def query_many(self, *exprs: Optional[QueryLike], data: Union[TableData, Iterable[QueryArgVals]]) -> Iterator[TableData]:
        for result in self.run_many(*exprs, data=data):
            if result is None:
                raise errors.NoResultsError('No results.')
            yield result

    def execute(self, *exprs: Optional[QueryLike], prms: Collection[ValueType] = ()) -> None:
        if self.run(*exprs, prms=prms) is not None:
            raise errors.ResultExistsError('Result exists.')

    def execute_many(self, *exprs: Optional[QueryLike], data: Union[TableData, Iterable[QueryArgVals]]) -> None:
        for _result in self.run_many(*exprs, data=data):
            if _result is not None:
                raise errors.ResultExistsError('Result exists.')

    def run(self, *exprs: Optional[QueryLike], prms: Optional[Collection[ValueType]]=None) -> Optional[TableData]:
        """ Run with optional single parameters """
        # Make QueryData
        qd = exprs[0] if len(exprs) == 1 and isinstance(exprs[0], QueryData) and not prms else QueryData(*exprs, prms=prms)
        # Run and handle result
        return self.run_stmt_prms(qd.stmt, qd.prms)

    def run_many(self, *exprs: Optional[QueryLike], data: Union[TableData, Iterable[QueryArgVals]]) -> Iterator[Optional[TableData]]:
        """ Run with multiple list of parameters """
        # Make QueryData
        qd = exprs[0] if len(exprs) == 1 and isinstance(exprs[0], QueryData) and not data else QueryData(*exprs)
        # Make argument values iterator
        iter_QueryArgVals = data.iter_rows_dict() if isinstance(data, TableData) else data
        # Run and handle result
        return self.run_stmt_many_prms(qd.stmt, qd.calc_prms_many(iter_QueryArgVals))

    @abstractmethod
    def commit(self) -> None:
        """ Commit the current transaction
        """

    @abstractmethod
    def last_row_id(self) -> int:
        """ Get a last inserted row id """

    # @property
    # def last_qd(self) -> Optional[QueryData]:
    #     return self._last_qd

    # def _handle_result(self, result, has_result: bool):
    #     if has_result:
    #         if result is None:
    #             raise errors.NoResultsError('No results.')
    #         return result
    #     else:
    #         if result is not None:
    #             raise errors.ResultExistsError('Result exists.')
    #     return None
