"""
    Test View object
"""
import libsql

def test_create_table():
    db = libsql.mysql.connect(user='testuser', password='testpass', database='testdb')
    
    if (_ext_table := db.table_or_none('students')) is not None:
        _ext_table.drop(if_exists=True)

    students = db.append_table('students')
    students.append_column('id', int, primary=True)
    students.append_column('name', str(64), not_null=True)
    students.create(drop_if_exists=True)
