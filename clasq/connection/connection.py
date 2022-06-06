"""
    SQL Connection classes and functions
"""
from __future__ import annotations
from abc import abstractmethod
from typing import Collection, Iterable, Iterator

from ..syntax.abc.exprs import SQLValue, QueryLike, QueryArgParams
from ..syntax.abc.object import NameLike
from ..syntax.abc.values import SQLValue
from ..utils.tabledata import TableData
from ..schema.abc.table import TableArgs
from ..schema.abc.column import TableColumnArgs
from ..schema.column import make_table_column_type
from ..schema.database import Database
from ..syntax.abc.query import QueryDataABC
from ..syntax.query import QueryData
from . import errors


class ConnectionABC:
    """ Database connection ABC """

    def __init__(self, database: str | Database | None = None,
                #  database_class: Optional[Type[DatabaseClass]] = None,
                 init_db=True, **other_cnx_options) -> None:
        """ Init """
        self._cnx_options = {**other_cnx_options}
        self._db: Database | None = None

        if isinstance(database, Database):
            self._cnx_options['database'] = database.raw_name
            self._db = database

        elif isinstance(database, str):
            self._cnx_options['database'] = database
            self._db = Database(database)
            self._db.connect(self)
            for table_args in self.iter_tables_args():
                self._db.append_table(table_args)

    @property
    def db(self) -> Database:
        if self._db is None:
            raise RuntimeError('Database is not initialized.')
        return self._db

    def set_db(self, db: Database | None) -> None:
        if db is None:
            if self._db is not None:
                self._db = None
        else:
            if self._db is None:
                self._db = db
                self._use_db(db._name)
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
    def run_stmt_prms(self, stmt: bytes, prms: Collection[SQLValue] = ()) -> TableData | None:
        """ Execute a query with single list of params and get result if exists """

    @abstractmethod
    def run_stmt_many_prms(self, stmt: bytes, prms_list: Iterable[Collection[SQLValue]]) -> Iterator[TableData | None]:
        """ Execute a query with multiple lists of params and get result if exists """

    def query(self, *exprs: QueryLike | None, prms: Collection[SQLValue] = ()) -> TableData:
        if (result := self.run(*exprs, prms)) is None:
            raise errors.NoResultsError('No results.')
        return result

    def query_many(self, *exprs: QueryLike | None, data: TableData | Iterable[QueryArgParams]) -> Iterator[TableData]:
        for result in self.run_many(*exprs, data=data):
            if result is None:
                raise errors.NoResultsError('No results.')
            yield result

    def execute(self, *exprs: QueryLike | None, prms: Collection[SQLValue] = ()) -> None:
        if self.run(*exprs, prms=prms) is not None:
            raise errors.ResultExistsError('Result exists.')

    def execute_many(self, *exprs: QueryLike | None, data: TableData | Iterable[QueryArgParams]) -> None:
        for _result in self.run_many(*exprs, data=data):
            if _result is not None:
                raise errors.ResultExistsError('Result exists.')

    def run(self, *exprs: QueryLike | None, prms: Collection[SQLValue] | None = None) -> TableData | None:
        """ Run with optional single parameters """
        # Make QueryData
        if len(exprs) == 1 and isinstance(exprs[0], QueryDataABC) and not prms:
            qd = exprs[0]
        else:
            qd = QueryData(*exprs, prms=prms)
        # Run and handle result
        return self.run_stmt_prms(qd.stmt, qd.prms)

    def run_many(self, *exprs: QueryLike | None, data: TableData | Iterable[QueryArgParams]) -> Iterator[TableData | None]:
        """ Run with multiple list of parameters """
        # Make QueryData
        if len(exprs) == 1 and isinstance(exprs[0], QueryDataABC) and not data:
            qd = exprs[0]
        else:
            qd = QueryData(*exprs)
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

    def iter_tables_args(self) -> Iterator[TableArgs]:
        """ Fetch tables of this database from the connection """
        for tabledata in self.query(b'SHOW', b'TABLES'):
            table_name = str(tabledata[0]).encode()
            yield TableArgs(table_name, tuple(
                TableColumnArgs(
                    name = coldata['Field'],
                    column_type = make_table_column_type(
                        data_type_sql=coldata['Type'],
                        nullable=coldata['Null'] == 'YES',
                        is_unique=coldata['Key'] == 'UNI',
                        is_primary=coldata['Key'] == 'PRI',
                        is_auto_increment='auto_increment' in coldata['Extra'],
                    ),
                    default = coldata['Default'],
                )
                for coldata in self.query(b'SHOW', b'COLUMNS', b'FROM', table_name)
            ))


    # @property
    # def last_qd(self) -> QueryData | None:
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
