"""
    Data-view result module
"""
from typing import Callable, Iterator, List, Tuple, Optional


class ResultIterator:

    def __init__(self, recitr:Iterator[Tuple], colnames_facs:List[Tuple[str, Optional[Callable]]], *, nextidx:int=0):
        # TODO: Implementation
        self.recitr = recitr
        
        self.colnames = [(tuple(tables), col) for *tables, col in (c.split('.') for c, _ in colnames_facs)]

        assert len(self.colnames) == set(self.colnames), "Duplicate column names."
        self.tables_colname2i = {tables: {colname:i for i, (_tables, colname) in enumerate(self.colnames) if _tables == tables} for tables in set(t for t, _ in self.colnames)}

        self.factories = [f for _, f in colnames_facs]

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

    # def colidx(self, colname:str, *, tables:Optional[Tuple[str]]=None):
    #     _tables:Tuple[str] = tables or tuple()
    #     return self.tables_colname2i[_tables][colname]

    # def colbyidx(self, idx:int):
    #     raw = self.row[idx]
    #     fac = self.factories[idx]
    #     if fac:
    #         return fac(raw)
    #     return raw

    # def col(self, colname:str, *, tables:Optional[Tuple[str]]=None):
    #     return self.colbyidx(self.colidx(colname, tables=tables))
