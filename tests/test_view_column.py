"""
    Test View and Column objects
"""
import itertools
from typing import List
import pytest

import libsql
from libsql.schema.column import TableColumn, ViewColumn
from libsql.syntax.exprs import Arg
from libsql.syntax.errors import ObjectNotFoundError, QueryArgumentError
from libsql.syntax.object_abc import ObjectName
from libsql.utils.tabledata import TableData
from libsql.syntax.keywords import OrderType

TABLE_COLUMN_NAMES = [
    ['products', ['id', 'category_id', 'name', 'price']],
    ['categories', ['id', 'name']],
    ['user_sale_products', ['id', 'user_sale_id', 'product_id', 'price', 'count']],
    ['user_sales', ['id', 'user_id', 'datetime']],
    ['users', ['id', 'name', 'registered_dt']],
]
TABLE_NAMES = [tablename for tablename, _ in TABLE_COLUMN_NAMES]
ALL_TABLE_COLUMN_NAMES = list(itertools.chain.from_iterable(
    [(tname, cname) for cname in cnames] for tname, cnames in TABLE_COLUMN_NAMES))


@pytest.mark.parametrize('tablename', TABLE_NAMES)
def test_table_get(tablename: str):
    db = libsql.mysql.connect(user='testuser', password='testpass', database='testdb')

    table = db[tablename]

    assert str(table.name) == tablename

    assert db.table(tablename) is table
    assert db.table_or_none(tablename) is table
    assert db.to_table(tablename) is table
    assert db.to_table_or_none(tablename) is table
    assert db.to_table(table) is table
    assert db.to_table_or_none(table) is table
    assert tablename in db
    assert table in db

    assert db[tablename.encode()] is table
    assert db.table(tablename.encode()) is table
    assert db.table_or_none(tablename.encode()) is table
    assert db[ObjectName(tablename)] is table
    assert db.table(ObjectName(tablename)) is table
    assert db.table_or_none(ObjectName(tablename)) is table


@pytest.mark.parametrize(('tablename', 'colnames'), TABLE_COLUMN_NAMES)
def test_table_column_get(tablename: str, colnames: List[str]):
    db = libsql.mysql.connect(user='testuser', password='testpass', database='testdb')

    table = db[tablename]
    assert all(isinstance(c, ViewColumn) for c in table.columns)
    assert all(c.base_view is table for c in table.columns)
    assert all(isinstance(c.expr, TableColumn) and c.expr.table is table for c in table.columns)
    assert [str(c.name) for c in table.columns] == colnames

    columns = list(table.columns)
    assert columns == [db[tablename][name] for name in colnames]
    assert columns == [table[name] for name in colnames]
    assert columns == [table.column(name) for name in colnames]
    assert columns == [table.col(name) for name in colnames]
    assert columns == [table.column_or_none(name) for name in colnames]
    assert columns == [table.to_column(name) for name in colnames]
    assert columns == [table.to_column_or_none(name) for name in colnames]
    assert columns == [table.to_column(col) for col in table.columns]
    assert columns == [table.to_column_or_none(col) for col in table.columns]
    assert columns == list(table[tuple(colnames)])
    assert list(reversed(columns)) == [table[name] for name in reversed(colnames)]
    
    assert columns == [table[name.encode()] for name in colnames]
    assert columns == [table[ObjectName(name)] for name in colnames]
    assert columns == [table.column(name.encode()) for name in colnames]
    assert columns == [table.column(ObjectName(name)) for name in colnames]

    assert all(name in table for name in colnames)
    assert all(column in table for column in columns)

    with pytest.raises(ObjectNotFoundError):
        table['qwerty']
    with pytest.raises(ObjectNotFoundError):
        table.to_column('qwerty')
    assert table.column_or_none('qwerty') is None
    assert table.to_column_or_none('qwerty') is None
    assert 'qwerty' not in table


@pytest.mark.parametrize('tablename, colnames', TABLE_COLUMN_NAMES)
def test_table_view_from_table(tablename, colnames):
    db = libsql.mysql.connect(user='testuser', password='testpass', database='testdb')

    table = db[tablename]
    columns = list(table.columns)

    view = table.where(table[colnames[0]] == 1)

    assert all(isinstance(c, ViewColumn) for c in view.columns)
    assert all(c.base_view is table for c in view.columns)
    assert all(isinstance(c.expr, TableColumn) and c.expr.table is table for c in view.columns)
    assert all(str(c.name) == name for c, name in zip(view.columns, colnames))

    assert columns == list(view.columns)
    assert columns == [view[name] for name in colnames]
    assert list(reversed(columns)) == [view[name] for name in reversed(colnames)]
    assert all(view.to_column(name) is column for column, name in zip(columns, colnames))
    assert all(view.to_column(view[name]) is column for column, name in zip(columns, colnames))

    ordered_table = table.order_by(columns[0])
    assert all(isinstance(c, ViewColumn) and c.base_view is table for c in ordered_table.columns)
    assert columns == [ordered_table[name] for name in colnames]

    ordered_view = view.order_by(columns[0])
    assert all(isinstance(c, ViewColumn) and c.base_view is table for c in ordered_view.columns)
    assert columns == [ordered_view[name] for name in colnames]


@pytest.mark.parametrize('tablename, colname', ALL_TABLE_COLUMN_NAMES)
def test_table_view_from_table_column(tablename: str, colname: str):
    db = libsql.mysql.connect(user='testuser', password='testpass', database='testdb')

    table = db[tablename]

    column = table[colname]

    assert str(column.name) == colname

    view = table.where(column == 1)

    assert view.select_query == table.where(**{colname: 1}).select_query
    assert view.result == table.where(**{colname: 1}).result

    otable_a = table.order_by(column)
    assert all(isinstance(c, ViewColumn) and c.base_view is table for c in otable_a.columns)
    assert otable_a[colname] is column
    assert otable_a.to_column(column) is column

    assert otable_a.select_query == table.order_by(+column).select_query
    assert otable_a.result       == table.order_by(+column).result
    assert otable_a.select_query == table.order_by(**{colname: True}).select_query
    assert otable_a.result       == table.order_by(**{colname: True}).result

    otable_d = table.order_by(-column)
    assert all(isinstance(c, ViewColumn) and c.base_view is table for c in otable_a.columns)
    assert otable_d[colname] is column
    assert otable_d.to_column(column) is column

    assert otable_d.select_query == table.order_by(-column).select_query
    assert otable_d.result       == table.order_by(-column).result
    assert otable_d.select_query == table.order_by(**{colname: False}).select_query
    assert otable_d.result       == table.order_by(**{colname: False}).result

    oview = view.order_by(column)
    assert all(isinstance(c, ViewColumn) and c.base_view is table for c in oview.columns)
    assert oview[colname] is column
    assert oview.to_column(column) is column

    assert oview.select_query == table.where(column == 1).order_by(column).select_query
    assert oview.select_query == view.order_by(view[colname]).select_query


@pytest.mark.parametrize('tablename, colnames', TABLE_COLUMN_NAMES)
def test_table_view_from_table_column_select(tablename: str, colnames: List[str]):
    db = libsql.mysql.connect(user='testuser', password='testpass', database='testdb')

    table = db[tablename]
    coln0 = colnames[0]
    col0 = table[coln0]

    s_table = table.select_column(col0)
    assert list(s_table.columns) == [col0]
    assert s_table[coln0] is col0
    assert col0 in s_table
    assert coln0 in s_table
    assert s_table.column(coln0) is col0
    assert s_table.column_or_none(coln0) is col0
    assert s_table.to_column(coln0) is col0
    assert s_table.to_column(col0) is col0
    assert s_table.to_column_or_none(coln0) is col0
    assert s_table.to_column_or_none(col0) is col0

    assert all(name not in s_table for name in colnames[1:])
    assert all(table[name] not in s_table for name in colnames[1:])
    assert all(s_table.column_or_none(name) is None for name in colnames[1:])
    assert all(s_table.to_column_or_none(name) is None for name in colnames[1:])
    assert all(s_table.to_column_or_none(table[name]) is None for name in colnames[1:])

    for name in colnames[1:]:
        with pytest.raises(ObjectNotFoundError):
            col = s_table[name]
        with pytest.raises(ObjectNotFoundError):
            col = s_table.to_column(name)

    assert len(table.result) == len(s_table.result)
