"""
    Test View and Column objects
"""
import itertools
import pytest

from libsql.connection import MySQLConnection
from libsql.schema.column import NamedViewColumnABC, TableColumn
from libsql.syntax.errors import ObjectNotFoundError
from libsql.syntax.abc.object import ObjectName

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
    db = MySQLConnection(user='testuser', password='testpass', database='testdb').db

    table = db[tablename]

    assert str(table.get_name()) == tablename

    assert db.get_table(tablename) is table
    assert db.get_table_or_none(tablename) is table
    assert db._to_table(tablename) is table
    assert db._to_table_or_none(tablename) is table
    assert db._to_table(table) is table
    assert db._to_table_or_none(table) is table
    assert tablename in db
    assert table in db

    assert db[tablename.encode()] is table
    assert db.get_table(tablename.encode()) is table
    assert db.get_table_or_none(tablename.encode()) is table
    assert db[ObjectName(tablename)] is table
    assert db.get_table(ObjectName(tablename)) is table
    assert db.get_table_or_none(ObjectName(tablename)) is table


@pytest.mark.parametrize(('tablename', 'colnames'), TABLE_COLUMN_NAMES)
def test_table_column_get(tablename: str, colnames: list[str]):
    db = MySQLConnection(user='testuser', password='testpass', database='testdb').db

    table = db[tablename]
    assert all(isinstance(c, NamedViewColumnABC) for c in table._selected_exprs)
    assert all(isinstance(c, TableColumn) and c.table is table for c in table._selected_exprs)
    assert [str(c.get_name()) for c in table._selected_exprs] == colnames

    columns = list(table._selected_exprs)
    assert columns == [db[tablename][name] for name in colnames]
    assert columns == [table[name] for name in colnames]
    assert columns == [table.get_column(name) for name in colnames]
    assert columns == [table.get_selected_column(name) for name in colnames]
    assert columns == [table.get_column_or_none(name) for name in colnames]
    assert columns == [table._to_column(name) for name in colnames]
    assert columns == [table._to_column(col) for col in table._selected_exprs]
    assert columns == [table._to_selected_column(name) for name in colnames]
    assert columns == [table._to_selected_column(col) for col in table._selected_exprs]
    assert columns == [table._to_column_or_none(name) for name in colnames]
    assert columns == [table._to_column_or_none(col) for col in table._selected_exprs]
    assert columns == list(table[tuple(colnames)])
    assert list(reversed(columns)) == [table[name] for name in reversed(colnames)]
    
    assert columns == [table[name.encode()] for name in colnames]
    assert columns == [table[ObjectName(name)] for name in colnames]
    assert columns == [table.get_column(name.encode()) for name in colnames]
    assert columns == [table.get_column(ObjectName(name)) for name in colnames]
    assert columns == [table.get_selected_column(name.encode()) for name in colnames]
    assert columns == [table.get_selected_column(ObjectName(name)) for name in colnames]

    assert all(name in table for name in colnames)
    assert all(column in table for column in columns)

    with pytest.raises(ObjectNotFoundError):
        table['qwerty']
    with pytest.raises(ObjectNotFoundError):
        table.get_column('qwerty')
    with pytest.raises(ObjectNotFoundError):
        table._to_column('qwerty')
    with pytest.raises(ObjectNotFoundError):
        table.get_selected_column('qwerty')
    with pytest.raises(ObjectNotFoundError):
        table._to_selected_column('qwerty')

    assert table.get_column_or_none('qwerty') is None
    assert table._to_column_or_none('qwerty') is None
    assert 'qwerty' not in table


@pytest.mark.parametrize('tablename, colnames', TABLE_COLUMN_NAMES)
def test_table_view_from_table(tablename, colnames):
    db = MySQLConnection(user='testuser', password='testpass', database='testdb').db

    table = db[tablename]
    columns = list(table._selected_exprs)

    view = table.where(table[colnames[0]] == 1)

    assert all(isinstance(c, NamedViewColumnABC) for c in view._selected_exprs)
    assert all(isinstance(c, TableColumn) and c.table is table for c in view._selected_exprs)
    assert all(str(c.name) == name for c, name in zip(view._selected_exprs, colnames))

    assert columns == list(view._selected_exprs)
    assert columns == [view[name] for name in colnames]
    assert list(reversed(columns)) == [view[name] for name in reversed(colnames)]
    assert all(view._to_column(name) is column for column, name in zip(columns, colnames))
    assert all(view._to_selected_column(name) is column for column, name in zip(columns, colnames))
    assert all(view._to_column(view[name]) is column for column, name in zip(columns, colnames))
    assert all(view._to_selected_column(view[name]) is column for column, name in zip(columns, colnames))

    ordered_table = table.order_by(columns[0])
    assert all(isinstance(c, NamedViewColumnABC) and c.base_view is table for c in ordered_table._selected_exprs)
    assert columns == [ordered_table[name] for name in colnames]

    ordered_view = view.order_by(columns[0])
    assert all(isinstance(c, NamedViewColumnABC) and c.base_view is table for c in ordered_view._selected_exprs)
    assert columns == [ordered_view[name] for name in colnames]


@pytest.mark.parametrize('tablename, colname', ALL_TABLE_COLUMN_NAMES)
def test_table_view_from_table_column(tablename: str, colname: str):
    db = MySQLConnection(user='testuser', password='testpass', database='testdb').db

    table = db[tablename]

    column = table[colname]

    assert str(column.name) == colname

    view = table.where(column == 1)

    assert view._select_query == table.where(**{colname: 1})._select_query # type: ignore
    assert view.result == table.where(**{colname: 1}).result # type: ignore

    otable_a = table.order_by(column)
    assert all(isinstance(c, NamedViewColumnABC) and c._named_view is table for c in otable_a._selected_exprs)
    assert otable_a[colname] is column
    assert otable_a.get_selected_column(colname) is column
    assert otable_a._to_column(column) is column
    assert otable_a._to_selected_column(column) is column

    assert otable_a._select_query == table.order_by(+column)._select_query
    assert otable_a.result       == table.order_by(+column).result
    assert otable_a._select_query == table.order_by(**{colname: True})._select_query
    assert otable_a.result       == table.order_by(**{colname: True}).result

    otable_d = table.order_by(-column)
    assert all(isinstance(c, NamedViewColumnABC) and c._named_view is table for c in otable_a._selected_exprs)
    assert otable_d[colname] is column
    assert otable_d.get_selected_column(colname) is column
    assert otable_d._to_column(column) is column
    assert otable_d._to_selected_column(column) is column

    assert otable_d._select_query == table.order_by(-column)._select_query
    assert otable_d.result       == table.order_by(-column).result
    assert otable_d._select_query == table.order_by(**{colname: False})._select_query
    assert otable_d.result       == table.order_by(**{colname: False}).result

    oview = view.order_by(column)
    assert all(isinstance(c, NamedViewColumnABC) and c._named_view is table for c in oview._selected_exprs)
    assert oview[colname] is column
    assert oview._to_column(column) is column

    assert oview._select_query == table.where(column == 1).order_by(column)._select_query
    assert oview._select_query == view.order_by(view[colname])._select_query


@pytest.mark.parametrize('tablename, colnames', TABLE_COLUMN_NAMES)
def test_table_view_from_table_column_select(tablename: str, colnames: list[str]):
    db = MySQLConnection(user='testuser', password='testpass', database='testdb').db

    table = db[tablename]
    columns = [table[name] for name in colnames]
    coln0 = colnames[0]
    col0 = table[coln0]

    s_table = table.select_column(col0)
    assert list(s_table._selected_exprs) == [col0]
    assert s_table[coln0] is col0
    assert col0 in s_table
    assert coln0 in s_table
    assert s_table.get_column(coln0) is col0
    assert s_table.get_column_or_none(coln0) is col0
    assert s_table._to_column(coln0) is col0
    assert s_table._to_column(col0) is col0
    assert s_table._to_column_or_none(coln0) is col0
    assert s_table._to_column_or_none(col0) is col0
    
    assert s_table.get_selected_column(coln0) is col0
    assert s_table.get_selected_column_or_none(coln0) is col0
    assert s_table._to_selected_column(coln0) is col0
    assert s_table._to_selected_column(col0) is col0
    assert s_table._to_selected_column_or_none(coln0) is col0
    assert s_table._to_selected_column_or_none(col0) is col0
    
    assert coln0 in s_table
    assert coln0 in s_table._selected_exprs
    assert col0 in s_table
    assert col0 in s_table._selected_exprs
    assert columns == [table.get_column(name) for name in colnames]
    assert columns == [table.get_column_or_none(name) for name in colnames]
    assert columns == [table._to_column(name) for name in colnames]
    assert columns == [table._to_column_or_none(name) for name in colnames]

    assert all(name not in s_table._selected_exprs for name in colnames[1:])
    assert all(table[name] not in s_table._selected_exprs for name in colnames[1:])
    assert all(s_table.get_selected_column_or_none(name) is None for name in colnames[1:])
    assert all(s_table._to_selected_column_or_none(name) is None for name in colnames[1:])
    assert all(s_table._to_selected_column_or_none(table[name]) is None for name in colnames[1:])

    for name in colnames[1:]:
        with pytest.raises(ObjectNotFoundError):
            col = s_table.get_selected_column(name)
        with pytest.raises(ObjectNotFoundError):
            col = s_table._to_selected_column(name)

    assert len(table.result) == len(s_table.result)
