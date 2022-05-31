"""
    Test TableData
"""
from typing import Any, Dict, List, Tuple
import pytest

from libsql.utils.tabledata import FrozenTableData, TableData, ColumnMetadata, RowData

@pytest.mark.parametrize('cols', [
    [],
    ['col1'],
    ['cl1', 'col2'],
    ['hoge', 'fugafuga', 'piyoo'],
    ['id', 'name', 'age', 'grade'],
])
def test_column_metadata(cols: List[str]):
    colmeta = ColumnMetadata([*cols])
    assert len(colmeta) == len(cols)
    assert colmeta.columns == tuple(cols)
    assert list(colmeta.iter_columns()) == cols
    assert list(iter(colmeta)) == cols
    assert list(colmeta) == cols
    assert all(colmeta.has_column(col) for col in cols)
    assert all(col in colmeta for col in cols)
    assert not colmeta.has_column('unknown')
    assert 'unknown' not in colmeta
    assert all(colmeta.column_to_i(col) == i for i, col in enumerate(cols))
    assert all(colmeta.column_to_i(col) == i for i, col in reversed(list(enumerate(cols))))

    with pytest.raises(KeyError):
        colmeta.column_to_i('unknown')

    colmeta_from_iter  = ColumnMetadata(iter(cols))
    assert colmeta == colmeta_from_iter
    assert not colmeta == ColumnMetadata(['another', 'columns'])

@pytest.mark.parametrize('row_dict', [
    {},
    {'col1': 'hoge'},
    {'cl1': 'foofoo', 'col2': 'barbarbar'},
    {'hoge': '', 'fugafuga': 0, 'flag': False},
    {'id': 25, 'name': 'John', 'age': 21, 'grade': 3},
])
def test_row_data(row_dict: Dict[str, Any]):
    row_from_dict = RowData(row_dict)
    row_from_cols = RowData(row_dict.keys(), row_dict.values())
    row_from_meta = RowData(ColumnMetadata(row_dict.keys()), row_dict.values())
    assert row_from_dict == row_from_cols
    assert row_from_dict == row_from_meta
    assert row_from_cols == row_from_meta
    for row in (row_from_dict, row_from_cols, row_from_meta):
        assert len(row) == len(row_dict)
        assert all(row[k] == v for k, v in row_dict.items())
        assert all(row[k] == v for k, v in reversed(row_dict.items()))
        assert all(row.get(k) == v for k, v in row_dict.items())
        assert list(row.columns) == list(row_dict.keys())
        assert list(row.raw_values) == list(row_dict.values())
        assert list(row.keys()) == list(row_dict.keys())
        assert list(row.values()) == list(row_dict.values())
        assert list(row.items()) == list(row_dict.items())
        assert row.asdict() == row_dict

@pytest.mark.parametrize('cols, data', [
    ([], []),
    (['value'], [('content',)]),
    (['value'], [('value1',)]),
    (['hoge'], [('value1',), ('value2',)]),
    (['words'], [('There',), ('are',), ('many',), ('rows',)]),
    (['words', 'len'], [('There', 5), ('are', 3), ('many', 4), ('rows', 4)]),
    (['words', 'len', 'weight'], [('There', 5, 3.55), ('are', 3, 2.932), ('many', 4, 10.24), ('rows', 4, 8.5)]),
])
def test_table_data(cols: List[str], data: List[Tuple[Any, ...]]):
    f_table = FrozenTableData(cols, [*data])
    w_table = TableData(cols, [*data])

    for table in (f_table, w_table):
        assert table.columns == tuple(cols)
        assert list(table.iter_columns()) == cols
        assert list(table.iter_rows_values()) == data
        assert len(table) == len(data)
        assert all(table[i].raw_values == row for i, row in enumerate(data))
        assert all(table[i].raw_values == row for i, row in reversed(list(enumerate(data))))
        assert all(table.row(i).raw_values == row for i, row in reversed(list(enumerate(data))))
        assert all(table.row_values(i) == row for i, row in reversed(list(enumerate(data))))
        assert table == FrozenTableData(cols, [*data])

    if data:
        ld = len(data)
        w_table.append(data[-1])
        assert len(w_table) == ld + 1
        assert all(w_table[i].raw_values == row for i, row in enumerate(data))
        assert w_table[ld].raw_values == data[-1]
        assert w_table[-1].raw_values == data[-1]
        assert w_table[ld-1] == w_table[ld]
        assert w_table[-2] == w_table[-1]

        w_table.append(TableData(cols, [*data]))
        assert len(w_table) == 2 * ld + 1
        assert all(w_table[i].raw_values == row for i, row in enumerate(data))
        assert all(w_table[ld+1+i].raw_values == row for i, row in enumerate(data))
        assert all(w_table[i] == w_table[ld+1+i] for i in range(ld))
