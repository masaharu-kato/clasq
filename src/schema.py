"""
    Database schema module
"""
from typing import Any, Dict, Iterator, List, NewType, Optional, Union, Set, Tuple, Type
import sys
import re
import weakref

import sqlparse
import ddlparse

from . import sqlexpr as sqe


def database_from_ddls(ddlstext:str) -> 'Database':
    """ Create this object by ddl text (CREATE TABLE sql texts) """
    ddltexts = sqlparse.split('\n'.join(l for l in ddlstext.splitlines() if not re.match(r'^\s*\-\-', l)))
    dbname = (re.match(r'^\s*USE\s+`?(\w+)`?', ddltexts[0], re.IGNORECASE) or [None, None])[1]
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

    return Database(dbname, tables, final=True)


TableName  = NewType('TableName', str)
ColumnName = NewType('ColumnName', str)


class ColumnRef:
    """ Reference of the column in the table """

    def __init__(self, table_name:TableName, column_name:ColumnName):
        self.table_name = table_name
        self.column_name = column_name


class Column(sqe.SQLExprType):
    """ Column schema class """
    name             : str
    data_type        : str
    not_null         : bool
    is_primary       : bool
    default          : Any
    comment          : str
    link_column_refs : List[ColumnRef]
    table            : 'Table'         # parent table object
    link_columns     : Set['Column']
    only_on_db       : bool            # The only table name in the database or not
    frozen           : bool

    def __init__(self,
        name:str, data_type:str, *, not_null:bool = False, is_primary:bool = False, default:Any = None,
        comment:Optional[str] = None, link_column_refs:Optional[List[ColumnRef]] = None
    ):
        self.name = name
        self.data_type = data_type
        self.not_null = not_null
        self.is_primary = is_primary
        self.default = default
        self.comment = comment
        self.link_column_refs = [] if link_column_refs is None else link_column_refs
        self.link_columns = []
        self.only_on_db = False
        self.frozen = False

    def finalize(self, table:'Table'):
        """ Resolve references in link columns """
        assert isinstance(table, Table)
        self.table = table
        self.link_columns = weakref.WeakSet(self.table.db.get_column_by_ref(cref) for cref in self.link_column_refs)
        self.frozen = True

    def unique_alias(self) -> str:
        """ Get a unique alias on the database """
        if self.only_on_db:
            return self.name
        return self.table.name + '.' + self.name

    def opt_unique_alias(self) -> Optional[str]:
        """ Get a unique alias on the database if it is needed """
        alias = self.unique_alias()
        if self.name == alias:
            return None
        return alias

    def __repr__(self):
        return f'<Column `{self.name}`>'

    def __hash__(self):
        return hash((hash(self.table), self.name))

    def vrepr(self):
        """ Dump the details of this column """
        return repr((self.name, self.data_type, self.link_columns))

    def __str__(self) -> str:
        return self.name

    def __sqlout__(self, swp:sqe.SQLWithParams):
        if not self.only_on_db:
            swp.append(self.table).append('.', raw=True)
        return swp.append(sqe.sqlobj(self.name))

    def dump(self):
        """ Dump column attributes for debug """
        print('  - {}\t{}'.format(self.name, self.data_type), end='')
        if self.link_columns:
            print(': Links to column: ', end='')
            for column in self.link_columns:
                print(column.table.name + '.' + column.name, end=' ')
        print('')


class Table(sqe.SQLExprType):
    """ Table schema class """
    db: 'Database'                                     # parent database object
    name: str                                          # table name
    _coldict : Dict[ColumnName, Column]                # dict of own columns (column name -> column object)
    _keycol  : Column                                  # primary key column
    parent_tables : Dict['Table', Tuple[Column, Column]] # parent table name -> own column object
    child_tables  : Dict['Table', Tuple[Column, Column]] # child table name -> child table's column object
    only_on_db    : bool                               # The only table name in the database or not
    frozen        : bool                               # Is frozen or not

    def __init__(self, name:TableName, columns:Optional[List[Column]]=None, *, record_class:Optional[Type]=None):
        self.db = None
        self.name = name
        self._coldict = {column.name: column for column in columns} if columns else {}
        assert all(isinstance(c, Column) for c in self._coldict.values())
        _keycols = [col for col in self._coldict.values() if col.is_primary]
        if len(_keycols) > 1:
            raise RuntimeError('Multiple primary keys.')
        self._keycol = _keycols[0] if _keycols else None
        self.parent_tables = {}
        self.child_tables  = {}
        self.only_on_db = True
        self.frozen = False
        self.record_class = record_class

    @property
    def columns(self) -> Iterator[Column]:
        """ Iterate all column objects """
        return self._coldict.values()

    @property
    def keycol(self) -> Column:
        """ Get primary key column """
        return self._keycol

    def add_column(self, column:Column):
        if self.frozen:
            raise RuntimeError('This table is frozen.')
        if column.name in self._coldict:
            raise RuntimeError('Column `{}` already exists in table `{}`'.format(column.name, self.name))
        self._coldict[column.name] = column

    def finalize(self, db:'Database'):
        """ Resolve references in _coldict """
        assert(isinstance(db, Database))
        self.db = db
        for column in self.columns:
            column.finalize(self)

        # Generate child and parent tables
        for column in self.columns:
            for destcol in column.link_columns:

                if destcol.table in self.child_tables:
                    raise RuntimeError(f'Multiple child links found between {column.table.name}.{column.name} and {destcol.table.name}.{destcol.name}')
                self.child_tables[destcol.table] = (column, destcol)

                if self in destcol.table.parent_tables:
                    raise RuntimeError(f'Multiple parent links found between {destcol.table.name}.{destcol.name} and {column.table.name}.{column.name}')
                destcol.table.parent_tables[self] = (destcol, column)

        self.frozen = True

    def column(self, column:Union[Column, ColumnName]) -> Column:
        """ Get column by column name / Check if column is valid """
        if isinstance(column, Column):
            if id(column.table) != id(self):
                raise RuntimeError(f'Unknown column `{column}`')
            return column

        if not isinstance(column, str):
            raise TypeError(f'Invalid type of column (type:{type(column)}).')

        if column not in self._coldict:
            raise KeyError(f'Undefined column `{column}`.')

        return self._coldict[column]

    def col(self, column_name:Union[Column, ColumnName]) -> Column:
        return self.column(column_name)

    def __getitem__(self, column_name:Union[Column, ColumnName]) -> Column:
        return self.column(column_name)

    # def get_table_links(self, dest_table:TableName) -> Iterator[Tuple[Column, Column]]:
    #     """ Get links (pairs of two table-_coldict) between this table and specific table """
    #     return self.db.get_table_links(self.name, dest_table)

    def find_tables_links(self, target_tables:Optional[List[TableName]]=None) -> Iterator[Tuple[Column, Column]]:
        """ Get links (pairs of two table-_coldict) between this table and specific tables """
        return self.db.find_tables_links(self.name, target_tables)

    # def get_parent_table_links(self) -> Iterator[Tuple['Table', Tuple[Column, Column]]]: # (table object, (left column, right column))
    #     """ Get parent table(s) recursively with (table object, the table always exists or not) """
    #     for table, (lcol, rcol) in self.parent_tables.items():
    #         yield (table, (lcol, rcol))
    #     for table, (lcol, rcol) in self.parent_tables.items():
    #         yield from table.get_parent_table_links()

    # def get_child_table_links(self) -> Iterator[Tuple['Table', Tuple[Column, Column]]]: # (table object, (left column, right column))
    #     """ Get child table(s) recursively with (table object, the table always exists or not) """
    #     for table, (lcol, rcol) in self.child_tables.items():
    #         yield (table, (lcol, rcol))
    #     for table, (lcol, rcol) in self.child_tables.items():
    #         yield from table.get_child_table_links()

    def __repr__(self):
        return f'<Table `{self.name}`>'

    def __hash__(self):
        return hash((hash(self.db), self.name))

    def vrepr(self):
        return repr((self.name, list(self.columns)))

    def __str__(self) -> str:
        return self.name

    def __sqlout__(self, swp:sqe.SQLWithParams):
        if not self.only_on_db:
            swp.append(self.db).append('.', raw=True)
        return swp.append(sqe.sqlobj(self.name))

    def dump(self):
        """ Dump table attributes for debug """
        print(f'- Table: {self.name} ({self.record_class})')
        for column in self.columns:
            column.dump()
        
        # if self.link_tables:
        #     print('  [Link tables]')
        #     for name, links in self.link_tables.items():
        #         for col1, col2 in links:
        #             print(f'    {name} ON {col1.table.name}.{col1.name} = {col2.table.name}.{col2.name}')


class Database(sqe.SQLExprType):
    """ Database schema class """
    name: str
    _tbldict: Dict[TableName, Table]
    _coldict: Dict[ColumnName, List[Column]]
    frozen: bool

    def __init__(self, name:str, tables:Optional[List[Table]]=None, *, final:bool=False):
        self.name = name
        self._tbldict = {table.name: table for table in tables} if tables else {}
        self._coldict = {}
        assert all(isinstance(t, Table) for t in self._tbldict.values())
        self.frozen = False
        if final:
            self.finalize()

    @property
    def tables(self) -> Iterator[Table]:
        """ Iterate all table objects """
        return self._tbldict.values()

    def add_table(self, table:Table):
        """ Add new table """
        if self.frozen:
            raise RuntimeError('This database is frozen.')
        if table.name in self._tbldict:
            raise RuntimeError('Table `{}` already exists.'.format(table.name))
        self._tbldict[table.name] = table

    def finalize(self):
        """ Resolve references in _tbldict """

        for table in self.tables:
            # Finalize table
            table.finalize(self)

            # Search columns in the tables
            for column in table.columns:
                if not column.name in self._coldict:
                    self._coldict[column.name] = []
                self._coldict[column.name].append(column)
        
        for columns in self._coldict.values():
            if len(columns) == 1:
                columns[0].only_on_db = True

        self.frozen = True
                
                
    def table(self, table:Union[Table, TableName]) -> Table:
        """ Get table object by table name / Check if table is valid """
        if isinstance(table, Table):
            if id(table.db) != id(self):
                raise RuntimeError(f'Unknown table `{table}`.')
            return table

        if not isinstance(table, str):
            raise TypeError(f'Invalid type of table (type:{type(table)}).')

        table_s = table.split('.')
        if len(table_s) >= 2: # Specified like 'database.table'
            table = table_s[-1]

        if table not in self._tbldict:
            raise KeyError(f'Undefined table `{table}`.')

        return self._tbldict[table]
    
    def __getitem__(self, table_name:Union[Table, TableName]) -> Table:
        return self.table(table_name)

    def column(self, column:Union[Column, ColumnName]) -> Column:
        """ Get column object by column name (Assume just one column) / Check if column is valid """
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

        cols = self._coldict[column]
        if len(cols) > 1:
            raise RuntimeError(f'Multiple columns found. (`{column}`)')

        return cols[0]

    def col(self, column_name:Union[Column, ColumnName]) -> Column:
        return self.column(column_name)

    def get_column_by_ref(self, column_ref:ColumnRef) -> Column:
        """ Get table-column entity by its reference object """
        return self[column_ref.table_name][column_ref.column_name]

    def find_tables_links(self, dest_table:TableName, target_tables:Optional[List[TableName]]=None) -> Iterator[Tuple[Column, Column]]:
        """ Get links (pairs of two table-columns) between a specific table and target _tbldict """
        _tbldict = self.tables if target_tables is None else target_tables
        for ctable in _tbldict:
            yield from self.get_table_links(dest_table, ctable)

    def get_table_links(self, base_table:TableName, dest_table:TableName) -> Iterator[Tuple[Column, Column]]:
        """ Get links (pairs of two table-columns) between two _tbldict """
        if dest_table in self[base_table].link_tables:
            yield from ((rcol, lcol) for lcol, rcol in self[base_table].link_tables[dest_table])
        if base_table in self[dest_table].link_tables:
            yield from self[dest_table].link_tables[base_table]
 
    def __repr__(self):
        return f'<Database `{self.name}`>'

    def __hash__(self):
        return hash(self.name)

    def vrepr(self):
        return repr((self.name, list(self.tables)))

    def __str__(self) -> str:
        return self.name

    def __sqlout__(self, swp:sqe.SQLWithParams):
        return swp.append(sqe.sqlobj(self.name))

    def dump(self):
        """ Dump database attributes for debug """
        print(f'Database {self.name}')
        for table in self.tables:
            table.dump()
            print('')


    # def __init__(self,
    #     table_columns:Dict[Table, List[Column]],
    #     table_links  :Dict[Table, Dict[Table, Column]],
    # ):
    #     self.table_columns = table_columns
    #     self.table_links = table_links


def main():
    """ Debug test function """
    with open(sys.argv[1], mode='r') as f:
        db = database_from_ddls(f.read())

    db.dump()


if __name__ == "__main__":
    main()
