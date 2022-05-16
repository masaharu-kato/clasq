"""
    Test View object
"""
import libsql
from libsql.schema.column import ColumnArgs

def test_create_table():
    db = libsql.mysql.connect(user='testuser', password='testpass', database='testdb')
    
    if (_ext_table := db.table_or_none('students')) is not None:
        _ext_table.drop(if_exists=True)

    students = db.append_table('students',
        ColumnArgs('id', int, primary=True),
        ColumnArgs('name', str(64), not_null=True),
    )
    assert 'students' in db
    assert 'id' in db['students']
    assert 'name' in db['students']
    
    students.create(drop_if_exists=True)
