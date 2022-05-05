"""
    Test View object
"""
import libsql

def test_create_table():
    db = libsql.mysql.connect(user='testuser', password='testpass', database='testdb')

    categories = db.append_table('students')
    categories.append_column('id', int, primary=True)
    categories.append_column('name', str(64), not_null=True)
    categories.create(drop_if_exists=True)
