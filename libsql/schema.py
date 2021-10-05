"""
    Database schema module
"""
from abc import abstractmethod
from typing import Any, DefaultDict, Dict, Iterator, List, NewType, Optional, Union, Set, Sequence, Tuple, Type
import sys
import re
import weakref

from enum import Enum
from .syntax.sql_expression import SQLExprType

import sqlparse # type: ignore
import ddlparse # type: ignore

_IS_DEBUG = True

def asobj(_objname) -> str:
    """ Format single object name """
    if _objname is None:
        return ''
    name = str(_objname)
    if _IS_DEBUG:
        if '`' in name:
            raise RuntimeError('Invalid character(s) found in the object name: %s' % name)
    return '`' + name + '`'

def joinobjs(*objs) -> str:
    return '.'.join(objs)

def joinasobjs(*objs) -> str:
    return joinobjs(*map(asobj, objs))

def asmultiobj(_objname) -> str:
    return joinasobjs(str(_objname).split('.'))

def astext(_text) -> str:
    """ Format single object name """
    name = '' if _text is None else str(_text)
    if _IS_DEBUG:
        if '"' in name:
            raise RuntimeError('Invalid character(s) found in the text.')
    return '"' + name + '"'

def astype(typename) -> str:
    """ Format SQL type """
    if _IS_DEBUG:
        if not re.match(r'\w+(\(\w*\))?', typename):
            raise RuntimeError('Invalid typename "{}".'.format(typename))
    return typename


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

TableLike = Union['Table', TableName, str]
ColumnLike = Union['Column', ColumnName, str]

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
        name:Union[ColumnName, str], data_type:str, *, not_null:bool = False, is_primary:bool = False, default:Any = None,
        comment:Optional[str] = None, link_column_refs:Optional[Sequence[Union[ColumnRef, tuple, str]]] = None
    ):
        assert isinstance(name, str)
        assert isinstance(data_type, str)
        assert isinstance(not_null, bool)
        assert isinstance(is_primary, bool)
        self._table:Optional[Table] = None
        self.name = ColumnName(name)
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
        self._table = table
        self.link_columns = weakref.WeakSet(self.table.db.get_column_by_ref(cref) for cref in self.link_column_refs)

    @property
    def table(self) -> 'Table':
        if self._table is None:
            raise RuntimeError('Table is not set.')
        return self._table

    def unique_alias(self) -> str:
        """ Get a unique alias on the database """
        if self.only_on_db:
            return self.name
        return self.table.name + '.' + self.name

    def __repr__(self) -> str:
        if self._table is None:
            return '<Column ???.`%s`>' % self.name
        return '<Column `%s`.`%s`>' % (self.table.name, self.name)

    def __hash__(self) -> int:
        return hash((hash(self.table), self.name))

    def sql(self, *, table:bool=True) -> str:
        """ Get SQL expression of this column """
        return joinobjs(self.table.sql(), asobj(self.name))


class TableLink:
    """ Linked table information """
    def __init__(self, origcol:Column, destcol:Column, is_nullable:bool, is_parent:bool):
        self.origcol = origcol
        self.destcol = destcol
        self.is_nullable = is_nullable
        self.is_parent = is_parent

    @property
    def lcol(self) -> Column:
        return self.origcol

    @property
    def rcol(self) -> Column:
        return self.destcol


class Table(SQLSchemaObjABC):
    """ Table schema class """          

    def __init__(self,
        table_name  : Union[TableName, str], # Table name
        columns     : Optional[List[Column]] = None, # List of columns
        *,
        record_type : Optional[Type] = None # Record class type
    ):
        assert isinstance(table_name, str)
        assert columns is None or all(isinstance(c, Column) for c in columns)

        self._db:Optional[Database] = None # parent database object
        self.name = TableName(table_name)            # table name

        # dict of own columns (column name -> column object)
        self._coldict = {column.name: column for column in columns} if columns is not None else {}
        
        # find primary key columns (assume single primary key)
        _keycols = [col for col in self._coldict.values() if col.is_primary]
        if len(_keycols) > 1:
            raise RuntimeError('Multiple primary keys on table %s' % table_name)
        if not len(_keycols):
            raise RuntimeError('No primary keys on table %s' % table_name)

        self._keycol = _keycols[0]

        self.tables_links:Dict[Table, List[TableLink]] = DefaultDict(lambda: [])   # linked tables
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
        self._db = db
        for column in self.columns:
            column._set_table_and_finalize(self)

        # Generate child and parent tables
        for origcol in self.columns:
            for destcol in origcol.link_columns:
                # print(origcol, destcol)
                self.tables_links[destcol.table].append(TableLink(origcol, destcol, is_nullable=not origcol.not_null, is_parent=False))
                destcol.table.tables_links[self].append(TableLink(destcol, origcol, is_nullable=not destcol.not_null, is_parent=True ))

    @property
    def db(self) -> 'Database':
        if self._db is None:
            raise RuntimeError('Database is not set.')
        return self._db

    def column(self, column:ColumnLike) -> Column:
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
        return self._coldict[ColumnName(column)]

    def col(self, column:ColumnLike) -> Column:
        """ Get column objects (alias of `self.column`) """
        return self.column(column)

    def __getitem__(self, column:ColumnLike) -> Column:
        return self.column(column)

    def find_tables_links(self, target_tables:Optional[Sequence[TableLike]]=None) -> Iterator[TableLink]:
        """ Get links (pairs of two table-_coldict) between this table and specific tables """
        return self.db.find_tables_links(self.name, target_tables)

    def __repr__(self) -> str:
        return '<Table `%s`>' % self.name

    def __hash__(self) -> int:
        return hash((hash(self.db), self.name))

    def sql(self) -> str:
        _o = asobj(self.name)
        return joinobjs(self.db.sql(), _o) if not self.only_on_dbs else _o

    def create_table_sql(self, *, exist_ok:bool=True) -> str:
        """ Get create table SQL """
        sql = ''
        if exist_ok:
            sql = f'DROP TABLE IF EXISTS {asobj(self.name)}\n'

        sts = []
        for col in self.columns:
            sts.append(f'  {asobj(col.name)} {col.data_type}')

        for table, links in self.tables_links.items():
            for link in links:
                sts.append('  ' + ('LEFT' if link.is_nullable else 'INNER') + f' JOIN {link.destcol.table.sql()} ON {link.origcol.sql()} = {link.destcol.sql()}')

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
                
    def table(self, table:TableLike) -> Table:
        """ Get table object by table name / Check if table is valid """
        if isinstance(table, Table):
            if id(table.db) != id(self):
                raise RuntimeError(f'Unknown table `{table}`.')
            return table

        # assert isinstance(table, str)
        # table = table.split('.')[-1]
        return self._tbldict[TableName(table)]
    
    def __getitem__(self, table:TableLike) -> Table:
        return self.table(table)

    def column(self, column:ColumnLike, *, tables:Optional[Sequence[TableLike]]=None) -> Column:
        """ Get column object by column name (Assume just one column) / Check if column is valid """
        # TODO: Implement `tables` option

        if isinstance(column, Column):
            if id(column.table.db) != id(self):
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
            raise RuntimeError('Multiple columns found for `%s`' % column)

        return cols[0]

    def col(self, column:ColumnLike, *, tables:Optional[Sequence[TableLike]]=None) -> Column:
        """ Get column objects (alias of `self.column`) """
        return self.column(column, tables=tables)

    def get_column_by_ref(self, column_ref:ColumnRef) -> Column:
        """ Get table-column entity by its reference object """
        assert isinstance(column_ref, ColumnRef)
        table = self[column_ref.table_name]
        if column_ref.column_name is None:
            return table.keycol
        return table[column_ref.column_name]

    def find_tables_links(self, dest_table:TableLike, target_tables:Optional[Sequence[TableLike]]=None) -> Iterator[TableLink]:
        """ Get links (pairs of two table-columns) between a specific table and target _tbldict """
        _tables:Sequence[TableLike] = list(self.tables) if target_tables is None else target_tables
        for table in _tables:
            yield from self.get_table_links(dest_table, table)

    def get_table_links(self, base_table:TableLike, dest_table:TableLike) -> Iterator[TableLink]:
        """ Get links (pairs of two table-columns) between two _tbldict """
        if self.table(dest_table) in self.table(base_table).tables_links:
            yield from self.table(base_table).tables_links[self.table(dest_table)]
        if base_table in self.table(dest_table).tables_links:
            yield from self.table(dest_table).tables_links[self.table(base_table)]
 
    def __repr__(self):
        return f'<Database `{self.name}`>'

    def __hash__(self):
        return hash(self.name)

    def sql(self) -> str:
        return asobj(self.name)

    def create_tables_sql(self, *, exist_ok:bool=True):
        """ Get tables creation SQL """
        return ''.join(table.create_table_sql(exist_ok=exist_ok) + ';\n' for table in self.tables)

ColumnAlias = NewType('ColumnAlias', str)
ColumnAs = Union[ColumnLike, Tuple[ColumnLike, ColumnAlias]]

ExtraColumnExpr = NewType('ExtraColumnExpr', str)

class JoinType(Enum):
    NONE = None
    INNER = 'INNER'
    LEFT  = 'LEFT'
    RIGHT = 'RIGHT'
    OUTER = 'OUTER'
    CROSS = 'CROSS'

class OrderType(Enum):
    NONE = None
    ASC  = 'ASC'
    DESC = 'DESC'

JoinLike = Union[JoinType, str]
OrderLike = Union[OrderType, str]



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
