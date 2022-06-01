"""
    Test sqltypes
"""
from typing import Any, Type, Literal as Lt
import datetime
import decimal
import pytest

from libsql.schema import sqltypes as sqt
from libsql.schema.sqltype_abc import SQLTypeABC
from libsql.utils.generic_cls import bind_generic_args

@pytest.mark.parametrize(('cls', 'name', 'val'), [
    (sqt.TinyInt,  b'TINYINT', -123),
    (sqt.SmallInt, b'SMALLINT', -12345),
    (sqt.MediumInt,  b'MEDIUMINT', -123456),
    (sqt.Int, b'INT', -12345678),
    (sqt.BigInt, b'BIGINT', -123456789012),
    (sqt.UnsignedTinyInt, b'TINYINT UNSIGNED', 123),
    (sqt.UnsignedSmallInt, b'SMALLINT UNSIGNED', 12345),
    (sqt.UnsignedMediumInt, b'MEDIUMINT UNSIGNED', 123456),
    (sqt.UnsignedBigInt, b'BIGINT UNSIGNED', 123456789012),
    (sqt.Float, b'FLOAT', 12.345),
    (sqt.Double, b'DOUBLE', 12.345),
    (sqt.Decimal[Lt[16], Lt[8]], b'DECIMAL(16, 8)', decimal.Decimal('123.4567')),
    (sqt.Decimal[Lt[12], Lt[5]], b'DECIMAL(12, 5)', decimal.Decimal('123.4567')),
    (sqt.Bit[Lt[1]], b'BIT(1)', 1),
    (sqt.Bit[Lt[10]], b'BIT(10)', 123),
    (sqt.DateTime, b'DATETIME', datetime.datetime(2012, 3, 4, 5, 40, 32)),
    (sqt.Date, b'DATE', datetime.date(2012, 3, 4)),
    (sqt.Time, b'TIME', datetime.time(5, 40, 32)),
    (sqt.Char[Lt[64]], b'CHAR(64)', 'hello, SQL.'),
    (sqt.Char[Lt[120]], b'CHAR(120)', 'hello, SQL.'),
    (sqt.VarChar[Lt[64]], b'VARCHAR(64)', 'hello, SQL.'),
    (sqt.VarChar[Lt[150]], b'VARCHAR(150)', 'hello, SQL.'),
    (sqt.VarChar[Lt[132]], b'VARCHAR(132)', 'hello, SQL.'),
    (sqt.Binary[Lt[64]], b'BINARY(64)', b'hello, binary SQL.'),
    (sqt.Binary[Lt[96]], b'BINARY(96)', b'hello, binary SQL.'),
    (sqt.VarBinary[Lt[96]], b'VARBINARY(96)', b'hello, binary SQL.'),
    (sqt.VarBinary[Lt[64]], b'VARBINARY(64)', b'hello, binary SQL.'),
    (sqt.Blob, b'BLOB', b'This is a sample BLOB content 12345'),
    (sqt.Blob[Lt[100]], b'BLOB(100)', b'This is a sample BLOB content 12345'),
    (sqt.Blob[Lt[345]], b'BLOB(345)', b'This is a sample BLOB content 12345'),
    (sqt.TinyBlob, b'TINYBLOB', b'This is a sample BLOB content 12345'),
    (sqt.MediumBlob, b'MEDIUMBLOB', b'This is a sample BLOB content 12345'),
    (sqt.LongBlob, b'LONGBLOB', b'This is a sample BLOB content 12345'),
    (sqt.Text, b'TEXT', 'This is a sample text 67890'),
    (sqt.Text[Lt[100]], b'TEXT(100)', 'This is a sample text 67890'),
    (sqt.Text[Lt[500]], b'TEXT(500)', 'This is a sample text 67890'),
    (sqt.TinyText, b'TINYTEXT', 'This is a sample text 67890'),
    (sqt.MediumText, b'MEDIUMTEXT', 'This is a sample text 67890'),
    (sqt.LongText, b'LONGTEXT', 'This is a sample text 67890'),
])
def test_sqltype_name(cls: Type[SQLTypeABC], name: bytes, val: Any):
    cls_with_args = bind_generic_args(cls)
    assert cls_with_args.sql_type_name == name
    assert cls_with_args.get_sql_type_name() == name
    assert cls_with_args.python_type is type(val)
    inst = cls(val)
    assert inst.cls.get_sql_type_name() == name
    assert inst.cls.sql_type_name == name
    assert inst.cls.python_type is type(val)
    assert inst.orig_value == val
    assert inst.sql_value == val



