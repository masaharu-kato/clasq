"""
    Database schema module
"""
from abc import abstractmethod
from typing import Any, Dict, Iterator, List, NewType, Optional, Union, Set, Sequence, Tuple, Type
import sys
import re
import weakref
from .syntax.sql_expression import SQLExprType

import sqlparse # type: ignore
import ddlparse # type: ignore

_IS_DEBUG = True

def asobj(_objname) -> str:
    """ Format single object name """
    name = str(_objname)
    if _IS_DEBUG:
        if '`' in name:
            raise RuntimeError('Invalid character(s) found in the object name.')
    return '`' + name + '`'

def joinobjs(*objs) -> str:
    return '.'.join(objs)

class SQLSchemaObjABC(SQLExprType):
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


TableName  = NewType('TableName', str)
ColumnName = NewType('ColumnName', str)


class ColumnRef:
    """ Reference of the column in the table """

    def __init__(self, table_name:TableName, column_name:Optional[ColumnName]=None):
        self.table_name = table_name
        self.column_name = column_name

    @classmethod
    def from_tuple(cls, arg):
        if isinstance(arg, ColumnRef):
            return arg
        if isinstance(arg, (tuple, list)):
            if len(arg) > 2:
                raise RuntimeError('Invalid initialize argument.')
            if len(arg) == 2:
                return  cls(arg[0], arg[1])
            return cls(arg[0])
        return cls(arg)

    def __repr__(self):
        if self.column_name is None:
            return '<ColumnRef `%s`.(key)>' % self.table_name
        return '<ColumnRef `%s`.`%s`>' % (self.table_name, self.column_name)


class Column(SQLSchemaObjABC):
    """ Column schema class """

    def __init__(self,
        name:str, data_type:str, *, not_null:bool = False, is_primary:bool = False, default:Any = None,
        comment:Optional[str] = None, link_column_refs:Optional[Sequence[Union[ColumnRef, tuple, str]]] = None
    ):
        assert isinstance(name, str)
        assert isinstance(data_type, str)
        assert isinstance(not_null, bool)
        assert isinstance(is_primary, bool)
        self.table:Optional[Table] = None
        self.name :ColumnName = name
        self.data_type = data_type
        self.not_null = not_null
        self.is_primary = is_primary
        self.default_value = default
        self.comment = comment
        self.link_column_refs = [] if link_column_refs is None else [ColumnRef.from_tuple(cref) for cref in link_column_refs]
        self.link_columns:weakref.WeakSet[Column] = weakref.WeakSet()
        self.only_on_db = False

    def _set_table_and_finalize(self, table:'Table') -> None:
        """ Set the parent table and finalize (resolve references in self.link columns) """
        assert isinstance(table, Table)
        self.table = table
        assert self.table.db is not None
        self.link_columns = weakref.WeakSet(self.table.db.get_column_by_ref(cref) for cref in self.link_column_refs)

    def unique_alias(self) -> str:
        """ Get a unique alias on the database """
        assert self.table is not None
        if self.only_on_db:
            return self.name
        return self.table.name + '.' + self.name

    def __repr__(self) -> str:
        if self.table is None:
            return '<Column ???.`%s`>' % self.name
        return '<Column `%s`.`%s`>' % (self.table.name, self.name)

    def __hash__(self) -> int:
        assert self.table is not None
        return hash((hash(self.table), self.name))

    def sql(self, *, table:bool=True) -> str:
        """ Get SQL expression of this column """
        assert self.table is not None
        return joinobjs(self.table.sql(), asobj(self.name))


class _LinkTable:
    """ Linked table information """
    def __init__(self, origcol:Column, destcol:Column, is_nullable:bool, is_parent:bool):
        self.origcol = origcol
        self.destcol = destcol
        self.is_nullable = is_nullable
        self.is_parent = is_parent


class Table(SQLSchemaObjABC):
    """ Table schema class """          

    def __init__(self,
        table_name  : TableName, # Table name
        columns     : Optional[List[Column]] = None, # List of columns
        *,
        record_type : Optional[Type] = None # Record class type
    ):
        assert isinstance(table_name, str)
        assert columns is None or all(isinstance(c, Column) for c in columns)

        self.db:Optional[Database] = None # parent database object
        self.name = table_name            # table name

        # dict of own columns (column name -> column object)
        self._coldict = {column.name: column for column in columns} if columns is not None else {}
        
        # find primary key columns (assume single primary key)
        _keycols = [col for col in self._coldict.values() if col.is_primary]
        if len(_keycols) > 1:
            raise RuntimeError('Multiple primary keys.')
        self._keycol = _keycols[0] if _keycols else None

        self.link_tables:Dict[Table, _LinkTable] = {}    # linked tables
        self.only_on_dbs = True  # The only table name in the databases or not
        self.record_class = record_type # Record class type

    @property
    def columns(self):
        """ Iterate all column objects """
        return self._coldict.values()

    @property
    def keycol(self) -> Column:
        """ Get primary key column """
        return self._keycol

    def _set_db_and_finalize(self, db:'Database'):
        """ Set the parent Database schema class 
            and finalize this table (Resolve references in self._coldict)
        """
        assert isinstance(db, Database)
        self.db = db
        for column in self.columns:
            column._set_table_and_finalize(self)

        # Generate child and parent tables
        for origcol in self.columns:
            for destcol in origcol.link_columns:
                # print(origcol, destcol)
                assert destcol.table is not None

                if destcol.table in self.link_tables:
                    raise RuntimeError(f'Multiple table links found between {origcol.sql()} and {destcol.sql()}')

                self.link_tables[destcol.table] = _LinkTable(origcol, destcol, is_nullable=not origcol.not_null, is_parent=False)
                destcol.table.link_tables[self] = _LinkTable(destcol, origcol, is_nullable=not destcol.not_null, is_parent=True)

    def column(self, column:Union[Column, ColumnName]) -> Column:
        """ Get column by column name / Check if column is valid
            When the Column object specified:
                The column is in this table: return the Column object
                Else: raise Error
            When the column name (assume string) specified:
                If the name of column exists: return the Column object
                Else: raise KeyError
        """
        if isinstance(column, Column):
            if id(column.table) != id(self):
                raise RuntimeError(f'Unknown column `{column}`')
            return column
        return self._coldict[column]

    def col(self, column_name:Union[Column, ColumnName]) -> Column:
        """ Get column objects (alias of `self.column`) """
        return self.column(column_name)

    def __getitem__(self, column_name:Union[Column, ColumnName]) -> Column:
        return self.column(column_name)

    def find_tables_links(self, target_tables:Optional[List[TableName]]=None) -> Iterator[Tuple[Column, Column]]:
        """ Get links (pairs of two table-_coldict) between this table and specific tables """
        assert self.db is not None
        return self.db.find_tables_links(self.name, target_tables)

    def __repr__(self) -> str:
        return '<Table `%s`>' % self.name

    def __hash__(self) -> int:
        assert self.db is not None
        return hash((hash(self.db), self.name))

    def sql(self) -> str:
        assert self.db is not None
        _o = asobj(self.name)
        return joinobjs(self.db.sql(), _o) if self.only_on_dbs else _o

    def create_table_sql(self, *, exist_ok:bool=True) -> str:
        """ Get create table SQL """
        sql = ''
        if exist_ok:
            sql = f'DROP TABLE IF EXISTS {asobj(self.name)}\n'

        sts = []
        for col in self.columns:
            sts.append(f'  {asobj(col.name)} {col.data_type}')

        for info in self.link_tables.values():
            sts.append('  ' + ('LEFT' if info.is_nullable else 'INNER') + f' JOIN {info.destcol.table.sql()} ON {info.origcol.sql()} = {info.destcol.sql()}')

        sql += f'CREATE TABLE {asobj(self.name)}(\n' + ',\n'.join(sts) + '\n)'
        return sql


class Database(SQLSchemaObjABC):
    """ Database schema class """

    def __init__(self,
        database_name : str,         # Database name
        tables        : List[Table], # List of Table schema objects
    ):
        assert tables is None or all(isinstance(t, Table) for t in tables)

        self.name = database_name
        self._tbldict = {table.name: table for table in tables} if tables else {}
        self._coldict:Dict[ColumnName, List[Column]] = {}

        # Resolve references in _tbldict
        for table in self.tables:
            # Finalize table
            table._set_db_and_finalize(self)

            # Search columns in the tables
            for column in table.columns:
                if not column.name in self._coldict:
                    self._coldict[column.name] = []
                self._coldict[column.name].append(column)
        
        for columns in self._coldict.values():
            if len(columns) == 1:
                columns[0].only_on_db = True

    @property
    def tables(self):
        """ Iterate all table objects """
        return self._tbldict.values()  
                
    def table(self, table:Union[Table, TableName, str]) -> Table:
        """ Get table object by table name / Check if table is valid """
        if isinstance(table, Table):
            if id(table.db) != id(self):
                raise RuntimeError(f'Unknown table `{table}`.')
            return table

        # assert isinstance(table, str)
        # table = table.split('.')[-1]
        return self._tbldict[TableName(table)]
    
    def __getitem__(self, table_name:Union[Table, TableName]) -> Table:
        return self.table(table_name)

    def column(self, column:Union[Column, ColumnName, str]) -> Column:
        """ Get column object by column name (Assume just one column) / Check if column is valid """
        if isinstance(column, Column):
            if column.table is None or id(column.table.db) != id(self):
                raise RuntimeError(f'Unknown column `{column}`.')
            return column

        if not isinstance(column, str):
            raise TypeError(f'Invalid type of column. (type:{type(column)})')

        col_s = column.split('.')
        if len(col_s) >= 2: # Specified like 'database.table.column' or 'table.column'
            return self.table(col_s[-2]).column(col_s[-1])

        if column not in self._coldict:
            raise KeyError(f'Undefined column `{column}`.')

        cols = self._coldict[ColumnName(column)]
        if len(cols) > 1:
            raise RuntimeError(f'Multiple columns found. (`{column}`)')

        return cols[0]

    def col(self, column_name:Union[Column, ColumnName]) -> Column:
        """ Get column objects (alias of `self.column`) """
        return self.column(column_name)

    def get_column_by_ref(self, column_ref:ColumnRef) -> Column:
        """ Get table-column entity by its reference object """
        assert isinstance(column_ref, ColumnRef)
        table = self[column_ref.table_name]
        if column_ref.column_name is None:
            return table.keycol
        return table[column_ref.column_name]

    def find_tables_links(self, dest_table:TableName, target_tables:Optional[List[TableName]]=None) -> Iterator[Tuple[Column, Column]]:
        """ Get links (pairs of two table-columns) between a specific table and target _tbldict """
        _tbldict = self.tables if target_tables is None else target_tables
        for ctable in _tbldict:
            yield from self.get_table_links(dest_table, ctable)

    def get_table_links(self, base_table:TableName, dest_table:TableName) -> Iterator[Tuple[Column, Column]]:
        """ Get links (pairs of two table-columns) between two _tbldict """
        if dest_table in self[base_table].link_tables:
            yield from ((rcol, lcol) for lcol, rcol in self[base_table].link_tables[self[dest_table]])
        if base_table in self[dest_table].link_tables:
            yield from self[dest_table].link_tables[self[base_table]]
 
    def __repr__(self):
        return f'<Database `{self.name}`>'

    def __hash__(self):
        return hash(self.name)

    def sql(self) -> str:
        return asobj(self.name)

    def create_tables_sql(self, *, exist_ok:bool=True):
        """ Get tables creation SQL """
        return ''.join(table.create_table_sql(exist_ok=exist_ok) + ';\n' for table in self.tables)


def database_from_ddls(ddlstext:str) -> 'Database':
    """ Create this object by ddl text (CREATE TABLE sql texts) """
    ddltexts = sqlparse.split('\n'.join(l for l in ddlstext.splitlines() if not re.match(r'^\s*\-\-', l)))
    dbname:Optional[str] = (re.match(r'^\s*USE\s+`?(\w+)`?', ddltexts[0], re.IGNORECASE) or [None, None])[1]
    tables = []

    for ddltext in ddltexts:
        if not re.match(r'^\s*CREATE TABLE.*\(', ddltext):
            continue
        ddlparser = ddlparse.DdlParse(ddltext)
        ddl = ddlparser.parse()
        tables.append(Table(
            ddl.name, [
                Column(
                    dcol.name,
                    dcol.data_type,
                    not_null = dcol.not_null,
                    is_primary = dcol.primary_key,
                    default = dcol.default,
                    comment = dcol.comment,
                    link_column_refs = [ColumnRef(dcol.fk_ref_table, dcol.fk_ref_column)] if dcol.foreign_key else None,
                ) for dcol in ddl.columns.values()
            ]
        ))

    return Database(dbname or '', tables)


def main():
    """ Debug test function """
    with open(sys.argv[1], mode='r') as f:
        db = database_from_ddls(f.read())

    db.dump()


if __name__ == "__main__":
    main()