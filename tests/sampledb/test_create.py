"""
    Test View object
"""
import clasq.connection
from clasq.schema.column import ColumnArgs
from clasq.schema.sqltypes import VarChar

def test_create_table():
    db = clasq.connection.MySQLConnection(user='testuser', password='testpass', database='testdb').db
    
    if (_ext_table := db.get_table_or_none('students')) is not None:
        _ext_table.drop(if_exists=True)

    students = db.append_table('students',
        ColumnArgs('id', int, primary=True),
        ColumnArgs('name', VarChar[64], nullable=True),
    )
    assert 'students' in db
    assert 'id' in db['students']
    assert 'name' in db['students']
    
    students.create(drop_if_exists=True)


def main():
    test_create_table()

if __name__ == '__main__':
    main()

