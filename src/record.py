from libsql.view import DataView

class Record:
    _dv: DataView
    _id: int

    def __init__(self, dv, _id, *args, **kwargs):
        # TODO: Implementation
        self._dv = dv
        self._id = _id

    def __getattr__(self, name):
        

    @property
    def _tables(self):
        return self._dv.new.tables

    @property
    def _db(self):
        return self._dv.db

    @property
    def id(self):
        return self._id
    
