"""
    Table data class
"""

from typing import Any, Iterator, List, Tuple, Union

TABLE_REPR_LIMIT = 100


class TableData:
    def __init__(self, columns: Union[List[str], 'ColumnMetadata'], rows: List[tuple]) -> None:
        """ Create a table data.

        Args:
            columns (List[str]): List of column names
            rows (List[list]): List of rows (values of columns)
        """
        self._col_meta = columns if isinstance(columns, ColumnMetadata) else ColumnMetadata(columns)
        
        if not (isinstance(rows, list) and all(isinstance(row, tuple) for row in rows)):
            raise TypeError('Invalid type of arguments.')

        self._rows = rows

    def __iter__(self) -> Iterator['RowData']:
        """ Iterate rows

        Yields:
            RowData: Row data class
        """
        for row in self._rows:
            yield RowData(self._col_meta, row)

    def __add__(self, value):
        if not isinstance(value, TableData):
            return NotImplemented
        table_data = value
        if not (self._col_meta == table_data._col_meta):
            raise ValueError('Cannot combine table data with different columns.')
        return TableData(self._col_meta, [*self._rows, *table_data._rows])

    def set_columns(self, columns: List[str]):
        self._col_meta = ColumnMetadata(columns)

    def copy_with_columns(self, columns: List[str]):
        return TableData(columns, self._rows)

    @property
    def columns(self):
        return self._col_meta.columns

    def iter_columns(self):
        return self._col_meta.iter_columns()

    def iter_rows_values(self):
        return iter(self._rows)

    def iter_rows_dict(self):
        for row in iter(self):
            yield dict(row.items())

    def rows_values_list(self):
        return list(self.iter_rows_values())

    def rows_dict_list(self):
        return list(self.iter_rows_dict())

    def __len__(self):
        return len(self._rows)

    def __getitem__(self, value):
        return RowData(self._col_meta, self._rows[value])

    def __eq__(self, val) -> bool:
        if not isinstance(val, TableData):
            return NotImplemented
        return self._col_meta == val._col_meta and self._rows == val._rows

    def make_html(self) -> str:
        return '<TABLE>\n' \
            + '<TR>' + ''.join('<TH>%s</TH>' % c for c in self.iter_columns()) + '</TR>\n' \
            + '\n'.join(('<TR>' + ''.join('<TD>%s</TD>' % v for v in row) + '</TR>') for row in self) \
            + '\n</TABLE>'

    def __repr__(self):
        res = 'TableData#%d\n' % id(self)
        # res += '\t'.join(str(c) for c in self._col_meta.iter_columns()) + '\n'
        # for i, row in enumerate(iter(self)):
        #     if i > TABLE_REPR_LIMIT:
        #         break
        #     res += '\t'.join(str(v) for v in row) + '\n'
        return res

    def _repr_html_(self):
        return self.make_html()


class ColumnMetadata:
    def __init__(self, columns: List[str]) -> None:
        """ Create a column metadata.

        Args:
            columns (List[str]): List of column names
        """
        self._cols = columns
        self._col_to_i = {col: i for i, col in enumerate(self._cols)}

    @property
    def columns(self):
        return self._cols

    def iter_columns(self) -> Iterator[str]:
        return iter(self._cols)

    def iter_indexes(self) -> Iterator[int]:
        return iter(self._col_to_i.values())

    def column_to_i(self, column: str) -> int:
        return self._col_to_i[column]

    def has_column(self, column: str) -> bool:
        return column in self._col_to_i

    def __contains__(self, column: str) -> bool:
        return self.has_column(column)

    def __eq__(self, value) -> bool:
        if not isinstance(value, ColumnMetadata):
            return NotImplemented
        col_meta = value
        return self._cols == col_meta._cols


class RowData:
    def __init__(self, column_metadata: ColumnMetadata, row: tuple) -> None:
        """ Create a row data.

        Args:
            column_metadata (ColumnMetadata): Column metadata.
            row (tuple): Row data (values of columns)
        """
        self._col_meta = column_metadata
        self._row = row

    def __getitem__(self, val: Union[int, str]) -> Any:
        """ Get a value of specific column

        Args:
            val (int | str): Index or Name of the column to get a value

        Returns:
            Any: Value of the specified column

        Raises:
            KeyError: The specified column is not found.
        """
        if isinstance(val, int):
            return self._row[val]
        return self._row[self._col_meta.column_to_i(val)]

    def get(self, val: Union[int, str], default: Any = None) -> Any:
        """ Get a value of specific column with a default value

        Args:
            column (int | str): Index or Name of the column to get a value
            default (Any, optional): Default value
                used if the specific column is not found. Defaults to None.

        Returns:
            Any: Value of the specified column, or a default value
        """
        if isinstance(val, int):
            return self._row[val] if val >= 0 and val < len(self._row) else default
        return self[val] if val in self._col_meta else default

    def __iter__(self) -> Iterator:
        """ Iterate row values

        Yields:
            Iterator: row value
        """
        # return (self._row[i] for i in self._col_meta.iter_indexes())
        return iter(self._row)

    def items(self) -> Iterator[Tuple[str, Any]]:
        """ Iterate columns with its name and value

        Yields:
            Iterator[Tuple[str, Any]]: Column name and its value
        """
        return zip(self._col_meta.iter_columns(), self._row)

    def make_dict(self):
        return dict(self.items())

    @property
    def __dict__(self):
        """ Generate a dictionary of this row

        Returns:
            dict: A dictionary of this row
        """
        return self.make_dict()
