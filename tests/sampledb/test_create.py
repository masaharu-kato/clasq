"""
    Test View object
"""
import libsql.connection.mysql
from libsql.schema.column import ColumnArgs
from libsql.schema.sqltypes import VarChar

def test_create_table():
    db = libsql.connection.mysql.connect(user='testuser', password='testpass', database='testdb')
    
    if (_ext_table := db.table_or_none('students')) is not None:
        _ext_table.drop(if_exists=True)

    students = db.append_table('students',
        ColumnArgs('id', int, primary=True),
        ColumnArgs('name', VarChar[64], nullable=True),
    )
    assert 'students' in db
    assert 'id' in db['students']
    assert 'name' in db['students']
    
    students.create(drop_if_exists=True)
