"""
    Test clasq.schema
"""
import pytest
from clasq.schema.column import TableColumn
from clasq.schema.database import Database
from clasq.schema.table import TableArgs
from clasq.schema.abc.column import TableColumnArgs as ColArgs
from clasq.syntax.data_types import Int, VarChar
from clasq.syntax.exprs import Object
from clasq.syntax.query import QueryData

@pytest.mark.parametrize('objname, result', [
    [b'hoGe', b'`hoGe`'],
    [b'B', b'`B`'],
    [b'?', b'`?`'],
    [b'hoge fugar', b'`hoge fugar`'],
    [b' piyo12fug', b'` piyo12fug`'],
    [b'bbb_cde ', b'`bbb_cde `'],
])
def test_asobj(objname, result):
    assert QueryData(Object(objname)) == QueryData(stmt=result)


# @pytest.mark.parametrize('objs, result', [
#     [['hogefu'], 'hogefu'],
#     [['hog', 'fuga'], 'hog.fuga'],
#     [['`hog`', '`fuga`'], '`hog`.`fuga`'],
#     [['hog.ab3r2', 'fuga', '`piyo.abc`'], 'hog.ab3r2.fuga.`piyo.abc`'],
# ])
# def test_joinobjs(objs, result):
#     assert ObjectExpr(*objs) == result


def test_table_columns():

    db = Database('mydb', (
        TableArgs('mytable', (
            ColArgs('id', TableColumn[Int]),
            ColArgs('mycol2', TableColumn[Int]),
            ColArgs('mycol3', TableColumn[VarChar[64]]),
        )),
        TableArgs('mytable2', (
            ColArgs('id', TableColumn[Int]),
            ColArgs('mycol4', TableColumn[Int]),
            ColArgs('mycol5', TableColumn[VarChar[64]]),
        )))
    )

    assert db.name == b'mydb'
    assert db['mytable']._name == b'mytable'
    assert db['mytable']['mycol2'].name == b'mycol2'
    assert db.get_table(b'mytable')['mycol2'].name == b'mycol2'
    assert db.get_table(b'mytable').get_column(b'mycol2').name == b'mycol2'
    assert db.get_table(b'mytable2').get_column(b'mycol5').name == b'mycol5'

    assert db['mytable']._name == b'mytable'
    assert QueryData(db['mytable']['mycol2']) == QueryData(stmt=b'`mytable`.`mycol2`')
