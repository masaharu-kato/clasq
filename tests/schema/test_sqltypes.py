"""
    Test sqltypes
"""
from typing import Type, Literal as Lt
import pytest

from libsql.schema import sqltypes as sqt
from libsql.schema.sqltype_abc import SQLTypeABC
from libsql.utils.generic_cls import bind_generic_args

@pytest.mark.parametrize(('cls', 'name'), [
    (sqt.TinyInt,  b'TINYINT'),
    (sqt.SmallInt, b'SMALLINT'),
    (sqt.MediumInt,  b'MEDIUMINT'),
    (sqt.Int, b'INT'),
    (sqt.BigInt, b'BIGINT'),
    (sqt.UnsignedTinyInt, b'TINYINT UNSIGNED'),
    (sqt.UnsignedSmallInt, b'SMALLINT UNSIGNED'),
    (sqt.UnsignedMediumInt, b'MEDIUMINT UNSIGNED'),
    (sqt.UnsignedBigInt, b'BIGINT UNSIGNED'),
    (sqt.Float, b'FLOAT'),
    (sqt.Double, b'DOUBLE'),
    (sqt.Decimal[Lt[16], Lt[8]], b'DECIMAL(16, 8)'),
    (sqt.Bit, b'BIT'),
    (sqt.DateTime, b'DATETIME'),
    (sqt.Date, b'DATE'),
    (sqt.Time, b'TIME'),
    (sqt.Char[Lt[64]], b'CHAR(64)'),
    (sqt.VarChar[Lt[64]], b'VARCHAR(64)'),
    (sqt.Binary[Lt[64]], b'BINARY(64)'),
    (sqt.VarBinary[Lt[64]], b'VARBINARY(64)'),
    (sqt.Blob, b'BLOB'),
    (sqt.Blob[Lt[100]], b'BLOB(100)'),
    (sqt.TinyBlob, b'TINYBLOB'),
    (sqt.MediumBlob, b'MEDIUMBLOB'),
    (sqt.LongBlob, b'LONGBLOB'),
    (sqt.Text, b'TEXT'),
    (sqt.Text[Lt[100]], b'TEXT(100)'),
    (sqt.TinyText, b'TINYTEXT'),
    (sqt.MediumText, b'MEDIUMTEXT'),
    (sqt.LongText, b'LONGTEXT'),
])
def test_sqltype_name(cls: Type[SQLTypeABC], name: bytes):
    cls_with_args = bind_generic_args(cls)
    assert cls_with_args.sql_type_name == name
    assert cls_with_args.get_sql_type_name() == name
