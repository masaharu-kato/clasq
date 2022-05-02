"""
    Table data class
"""

from typing import Any, Iterator, List, Tuple, Union

TABLE_REPR_LIMIT = 100

class TableData:
    def __init__(self, columns: Union[List[str], 'ColumnMetadata'], rows: List[list]) -> None:
        """ Create a table data.

        Args:
            columns (List[str]): List of column names
            rows (List[list]): List of rows (values of columns)
        """
        self._col_meta = columns if isinstance(columns, ColumnMetadata) else ColumnMetadata(columns)
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

    def __repr__(self):
        res = 'TableData#%d\n' % id(self)
        res += '\t'.join(str(c) for c in self._col_meta.iter_columns()) + '\n'
        for i, row in enumerate(iter(self)):
            if i > TABLE_REPR_LIMIT:
                break
            res += '\t'.join(str(v) for v in row) + '\n'
        return res


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
        return self._col_to_i.values()

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
    def __init__(self, column_metadata: ColumnMetadata, row: list) -> None:
        """ Create a row data.

        Args:
            column_metadata (ColumnMetadata): Column metadata.
            row (list): Row data (values of columns)
        """
        self._col_meta = column_metadata
        self._row = row

    def __getitem__(self, column: str) -> Any:
        """ Get a value of specific column

        Args:
            column (str): Name of the column to get a value

        Returns:
            Any: Value of the specified column

        Raises:
            KeyError: The specified column is not found.
        """
        return self._row[self._col_meta.column_to_i(column)]

    def get(self, column: str, default: Any = None) -> Any:
        """ Get a value of specific column with a default value

        Args:
            column (str): Name of the column to get a value
            default (Any, optional): Default value
                used if the specific column is not found. Defaults to None.

        Returns:
            Any: Value of the specified column, or a default value
        """
        return self[column] if column in self._col_meta else default

    def __iter__(self) -> Iterator:
        """ Iterate row values

        Yields:
            Iterator: row value
        """
        return (self._row[i] for i in self._col_meta.iter_indexes())

    def items(self) -> Iterator[Tuple[str, Any]]:
        """ Iterate columns with its name and value

        Yields:
            Iterator[Tuple[str, Any]]: Column name and its value
        """
        return zip(self._col_meta.iter_columns(), self._row)

    def __dict__(self) -> dict:
        """ Generate a dictionary of this row

        Returns:
            dict: A dictionary of this row
        """
        return dict(self.items())

