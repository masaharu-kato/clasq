"""
    Database schema module
"""
from collections import defaultdict
from typing import Dict, List, NewType, Tuple, Optional, Iterator
import sys
import re

import sqlparse
import ddlparse

TableName  = NewType('TableName', str)
ColumnName = NewType('ColumnName', str)


class ColumnRef:
    """ Reference of the column in the table """

    def __init__(self, table_name:TableName, column_name:ColumnName):
        self.table_name = table_name
        self.column_name = column_name


class Column:
    """ Column schema class """
    name: str
    data_type: str
    link_column_refs: List[ColumnRef]
    table: 'Table'
    link_columns: List['Column']

    def __init__(self, name, data_type, link_column_refs = None):
        self.name = name
        self.data_type = data_type
        self.link_column_refs = [] if link_column_refs is None else link_column_refs
        self.link_columns = []

    def resolve_references(self):
        """ Resolve references in link columns """
        if self.table is None:
            raise RuntimeError('This column does not belong to the table.')
        self.link_columns = [self.table.db.get_column_by_ref(cref) for cref in self.link_column_refs]

    def __repr__(self):
        return repr((self.name, self.data_type, self.link_columns))

    def __str__(self) -> str:
        return self.name

    def dump(self):
        """ Dump column attributes for debug """
        print('  - {}'.format(self.name), end='')
        if self.link_columns:
            print(': Links to column: ', end='')
            for column in self.link_columns:
                print(column.table.name + '.' + column.name, end=' ')
        print('')


class Table:
    """ Table schema class """
    db: 'Database'
    name: str
    _coldict: Dict[ColumnName, Column]
    link_tables: Dict[TableName, Tuple[Column, Column]]

    def __init__(self, name:TableName, _coldict:List[Column]):
        assert all(isinstance(c, Column) for c in _coldict)
        self.name = name
        self._coldict = {column.name: column for column in _coldict}
        assert all(isinstance(c, Column) for c in self.columns)

    @property
    def columns(self) -> Iterator[Column]:
        return self._coldict.values()

    def resolve_references(self):
        """ Resolve references in _coldict """
        if self.db is None:
            raise RuntimeError('This table does not belong to the database.')
        for column in self.columns:
            column.table = self
            column.resolve_references()

        self.link_tables = defaultdict(lambda: [])
        for self_column in self.columns:
            for link_column in self_column.link_columns:
                self.link_tables[link_column.table.name].append((self_column, link_column))

    def __getitem__(self, column_name:ColumnName) -> Column:
        """ Get column by column name """
        if column_name not in self._coldict:
            raise KeyError(f'Undefined column `{column_name}`.')
        return self._coldict[column_name]

    def get_table_links(self, dest_table:TableName) -> Iterator[Tuple[Column, Column]]:
        """ Get links (pairs of two table-_coldict) between this table and specific table """
        return self.db.get_table_links(self.name, dest_table)

    def find_tables_links(self, target_tables:Optional[List[TableName]]=None) -> Iterator[Tuple[Column, Column]]:
        """ Get links (pairs of two table-_coldict) between this table and specific tables """
        return self.db.find_tables_links(self.name, target_tables)

    def __repr__(self):
        return repr((self.name, list(self.columns)))

    def __str__(self) -> str:
        return self.name

    def dump(self):
        """ Dump table attributes for debug """
        print(f'- Table: {self.name}')
        for column in self.columns:
            column.dump()
        
        if self.link_tables:
            print('  [Link tables]')
            for name, links in self.link_tables.items():
                for col1, col2 in links:
                    print(f'    {name} ON {col1.table.name}.{col1.name} = {col2.table.name}.{col2.name}')


class Database:
    """ Database schema class """
    name: str
    _tbldict: Dict[TableName, Table]

    def __init__(self, name:str, _tbldict:List[Table], *, final:bool=False):
        assert all(isinstance(t, Table) for t in _tbldict)
        self.name = name
        self._tbldict = {table.name: table for table in _tbldict}
        assert all(isinstance(t, Table) for t in self.tables)
        if final:
            self.resolve_references()

    @property
    def tables(self) -> Iterator[Table]:
        return self._tbldict.values()

    def resolve_references(self):
        """ Resolve references in _tbldict """
        for table in self.tables:
            table.db = self
            table.resolve_references()

    def __getitem__(self, table_name:TableName) -> Table:
        """ Get table object by table name """
        if table_name not in self._tbldict:
            raise KeyError(f'Undefined table `{table_name}`.')
        return self._tbldict[table_name]
    
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
        return repr((self.name, list(self.tables)))

    def __str__(self) -> str:
        return self.name

    def dump(self):
        """ Dump database attributes for debug """
        print(f'Database {self.name}')
        for table in self.tables:
            table.dump()
            print('')


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
                    [ColumnRef(dcol.fk_ref_table, dcol.fk_ref_column)] if dcol.foreign_key else None
                ) for dcol in ddl.columns.values()
            ]
        ))

    return Database(dbname, tables, final=True)


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
