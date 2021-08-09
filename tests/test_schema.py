"""
    Test libsql.schema
"""
import pytest
from libsql import schema

@pytest.mark.parametrize('objname, result', [
    ['hoGe', '`hoGe`'],
    ['B', '`B`'],
    ['?', '`?`'],
    ['hoge fugar', '`hoge fugar`'],
    [' piyo12fug', '` piyo12fug`'],
    ['bbb_cde ', '`bbb_cde `'],
])
def test_asobj(objname, result):
    assert schema.asobj(objname) == result


@pytest.mark.parametrize('objs, result', [
    [['hogefu'], 'hogefu'],
    [['hog', 'fuga'], 'hog.fuga'],
    [['`hog`', '`fuga`'], '`hog`.`fuga`'],
    [['hog.ab3r2', 'fuga', '`piyo.abc`'], 'hog.ab3r2.fuga.`piyo.abc`'],
])
def test_joinobjs(objs, result):
    assert schema.joinobjs(*objs) == result


def test_table_columns():

    db = schema.Database('mydb', [
        schema.Table('mytable', [
            schema.Column('id', 'INT'),
            schema.Column('mycol2', 'INT'),
            schema.Column('mycol3', 'VARCHAR(64)'),
        ]),
        schema.Table('mytable2', [
            schema.Column('id', 'INT'),
            schema.Column('mycol4', 'INT'),
            schema.Column('mycol5', 'VARCHAR(64)'),
        ])
    ])

    assert db.name == 'mydb'
    assert db['mytable'].name == 'mytable'
    assert db['mytable']['mycol1'].name == 'mycol1'
    assert db.table('mytable')['mycol1'].name == 'mycol1'
    assert db.table('mytable').column('mycol2').name == 'mycol2'
    assert db.table('mytable2').col('mycol5').name == 'mycol5'

