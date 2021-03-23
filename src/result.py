"""
    Data-view result module
"""
from typing import Callable, Iterator, List, Tuple, Optional
from libsql.view import DataView


class ResultIterator:

    def __init__(self, recitr:Iterator[Tuple], colname_facs:List[Tuple[str, Optional[Callable]]], *, nextidx:int=0):
        # TODO: Implementation
        self.recitr = recitr
        
        self.colnames = [(tuple(tables), col) for *tables, col in (c.split('.') for c, _ in colnames_facs)]

        assert len(self.colnames) == set(self.colnames), "Duplicate column names."
        self.tables_colname2i = {tables: {colname:i for i, (_tables, colname) in enumerate(self.colnames) if _tables == tables} for tables in set(t for tables, _ in self.colnames)}

        self.factories = [f for _, f in colname_facs]

        self.crow = None
        self.nextidx = nextidx

    def next(self):
        try:
            self.crow = next(self.recitr)
        except StopIteration:
            self.crow = None
            self.nextidx = None
        else:
            self.nextidx += 1

    @property
    def row(self):
        if self.crow is None:
            raise RuntimeError('Iterator not available.')
        return self.crow

    @property
    def index(self):
        if self.nextidx:
            return self.nextidx - 1
        return None

    def colidx(self, colname:str, *, tables:Optional[Tuple[str]]=None):
        tables = tables or tuple()
        return self.tables_colname2i[tables][colname]

    def colbyidx(self, idx:int):
        raw = self.row[idx]
        fac = self.factories[idx]
        if fac:
            return fac(raw)
        return raw

    def col(self, colname:str, * tables:Optional[Tuple[str]]=None):
        return self.colbyidx(self.colidx(colname, tables=tables))

    def table(self, tablename:str):
        assert

        





class Result:
    _dv: DataView
    _recs: Iterator # Records received from the Database

    def __init__(self, dv, recs:Iterator[Tuple], colinfo:List[Tuple[Tuple[str], str]], *args, cidx:int=0, **kwargs):
        # TODO: Implementation
        self._dv = dv
        self._rec = recs
        self.nextidx = cidx

        self.colinfo = colinfo

        assert len(self.colnames) == set(self.colnames), "Duplicate column names."
        self.tables_colname2i = {tables: {colname:i for i, (_tables, colname) in enumerate(self.colnames) if _tables == tables} for tables in set(t for tables, _ in self.colnames)}




    @property
    def _db(self):
        return self._dv.db

    @property
    def _table(self):
        return self._db.table(self._tablename_)

    @property
    def _raw_tables(self) -> DataView:
        return self._dv.new.tables

    @property
    def _tables(self) -> DataView:
        return self._raw_tables[self._table == self._id]
    

    def _next(self):
        self._cidx += 1

    @property
    def _rawrec(self):
        return self._rawrecs[self._cidx]

    @property
    def _getcol(self, name:str):
        

    def __getattr__(self, name:str):
        NotImplemented

    @property
    def id(self):
        return self._id
