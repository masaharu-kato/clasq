"""
    Test View object
"""
from typing import Literal
import clasq.connection
from clasq.schema.abc.column import TableColumnArgs as ColArgs
from clasq.schema.column import PrimaryTableColumn, TableColumn
from clasq.syntax.data_types import Nullable, VarChar, Int

def test_create_table():
    db = clasq.connection.MySQLConnection(user='testuser', password='testpass', database='testdb').db
    
    if (_ext_table := db.get_table_or_none('students')) is not None:
        _ext_table.drop(if_exists=True)

    students = db.append_table('students', (
        ColArgs('id', PrimaryTableColumn[Int]),
        ColArgs('name', TableColumn[Nullable[VarChar[Literal[64]]]]),
    ))
    assert 'students' in db
    assert 'id' in db['students']
    assert 'name' in db['students']
    
    students.create(drop_if_exists=True)


def main():
    test_create_table()

if __name__ == '__main__':
    main()

