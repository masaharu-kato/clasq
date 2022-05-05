"""
    Test libsql.schema
"""
import pytest
from libsql.schema.database import Database, Table, Column
from libsql.syntax.sqltypes import Int, VarChar
from libsql.syntax.exprs import Object
from libsql.syntax.query_data import QueryData

@pytest.mark.parametrize('objname, result', [
    [b'hoGe', b'`hoGe`'],
    [b'B', b'`B`'],
    [b'?', b'`?`'],
    [b'hoge fugar', b'`hoge fugar`'],
    [b' piyo12fug', b'` piyo12fug`'],
    [b'bbb_cde ', b'`bbb_cde `'],
])
def test_asobj(objname, result):
    assert QueryData(Object(objname)) == QueryData(result)


# @pytest.mark.parametrize('objs, result', [
#     [['hogefu'], 'hogefu'],
#     [['hog', 'fuga'], 'hog.fuga'],
#     [['`hog`', '`fuga`'], '`hog`.`fuga`'],
#     [['hog.ab3r2', 'fuga', '`piyo.abc`'], 'hog.ab3r2.fuga.`piyo.abc`'],
# ])
# def test_joinobjs(objs, result):
#     assert ObjectExpr(*objs) == result


def test_table_columns():

    db = Database('mydb',
        Table('mytable',
            Column('id', Int),
            Column('mycol2', Int),
            Column('mycol3', VarChar[64]),
        ),
        Table('mytable2',
            Column('id', Int),
            Column('mycol4', Int),
            Column('mycol5', VarChar[64]),
        )
    )

    assert db.name == b'mydb'
    assert db['mytable'].name == b'mytable'
    assert db['mytable']['mycol2'].name == b'mycol2'
    assert db.table(b'mytable')['mycol2'].name == b'mycol2'
    assert db.table(b'mytable').column(b'mycol2').name == b'mycol2'
    assert db.table(b'mytable2').col(b'mycol5').name == b'mycol5'

    assert db['mytable'].name == b'mytable'
    assert QueryData(db['mytable']['mycol2']) == QueryData(b'`mytable`.`mycol2`')
