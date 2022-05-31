"""
    Table data class
"""
from __future__ import annotations
from typing import Dict, Generic, Iterable, List, Optional, Sequence, Tuple, TypeVar, Union, overload

from .tabledata_abc import ColumnMetadataABC, FrozenTableDataABC, TableDataABC, RowDataABC

T = TypeVar('T')

class FrozenTableData(FrozenTableDataABC[T], Generic[T]):
    """ Readonly Table data class """

    TABLE_REPR_LIMIT = 100

    def __init__(self, columns: Union[Iterable[str], 'ColumnMetadata'], rows: List[Tuple[T, ...]]) -> None:
        """ Create a table data.

        Args:
            columns (List[str]): List of column names
            rows (List[list]): List of rows (values of columns)
        """
        self._col_meta = columns if isinstance(columns, ColumnMetadata) else ColumnMetadata(columns)
        
        if not (isinstance(rows, list) and all(isinstance(row, tuple) for row in rows)):
            raise TypeError('Invalid type of arguments.')

        self._rows = rows

    @property
    def rows_values(self) -> Sequence[Tuple[T, ...]]:
        return self._rows

    @property
    def col_meta(self) -> ColumnMetadataABC:
        return self._col_meta

    def _new_row_data(self, col_meta: ColumnMetadataABC, row: Tuple[T, ...]) -> RowDataABC[T]:
        return RowData(col_meta, row)

    def __add__(self, value: TableDataABC[T]):
        """ Get a new TableData with another TableData

        Args:
            value (TableData): Another TableData object

        Raises:
            ValueError: Columns of tables are different

        Returns:
            TableData: A new TableData object
        """
        if not isinstance(value, TableDataABC):
            return NotImplemented
        table_data = value
        if not (self.col_meta == table_data.col_meta):
            raise ValueError('Cannot combine table data with different columns.')
        return FrozenTableData(self.col_meta, [*self.rows_values, *table_data.rows_values])

    def copy_with_columns(self, new_columns: Iterable[str]):
        """ Get a shallow copy of this table with new column names

        Args:
            new_columns (Iterable[str]): New column names

        Returns:
            TableData: New TableData object
        """
        return FrozenTableData(new_columns, list(self.rows_values))


class TableData(FrozenTableData[T], TableDataABC[T], Generic[T]):
    """ Table data class """

    @property
    def rows_values_list(self) -> List[Tuple[T, ...]]:
        return self._rows


class ColumnMetadata(ColumnMetadataABC):
    def __init__(self, columns: Iterable[str]) -> None:
        """ Create a column metadata.

        Args:
            columns (List[str]): List of column names
        """
        self.__cols = tuple(columns)
        self.__col_to_i = {col: i for i, col in enumerate(self.__cols)}

    @property
    def columns(self) -> Tuple[str, ...]:
        """ Get a tuple of all column names
            (Override for `ColumnMetadataABC`)

        Returns:
            Tuple[str]: Tuple of all column names
        """
        return self.__cols

    @property
    def _col_to_i(self) -> Dict[str, int]:
        return self.__col_to_i


class RowData(RowDataABC[T], Generic[T]):

    @overload
    def __init__(self, columns: Union[Iterable[str], ColumnMetadataABC], row: Iterable[T]) -> None:
        """ Create a row data

        Args:
            columns (Iterable[str] | ColumnMetadata): Column metadata or iterable of column names
            row (Iterable[T]): Row data (values of columns)
        """

    @overload
    def __init__(self, row_dict: Dict[str, T], /) -> None:
        """ Create a row data.

        Args:
            row_dict: A dictionary from column name to value
        """

    def __init__(self,
                 columns: Optional[Union[Iterable[str], ColumnMetadataABC, Dict[str, T]]] = None,
                 row: Optional[Iterable[T]] = None,
                 row_dict: Optional[Dict[str, T]] = None) -> None:

        if row_dict is not None and columns is None and row is None:
            col_meta: ColumnMetadataABC = ColumnMetadata(row_dict.keys())
            row_vals = tuple(row_dict.values())

        elif isinstance(columns, dict) and row is None and row_dict is None:
            col_meta = ColumnMetadata(columns.keys())
            row_vals = tuple(columns.values())

        elif isinstance(columns, (Iterable, ColumnMetadataABC)) and row is not None:
            col_meta = columns if isinstance(columns, ColumnMetadataABC) else ColumnMetadata(columns)
            row_vals = tuple(row)

        else:
            raise ValueError('Invalid arguments.')

        self.__col_meta = col_meta
        self.__row = row_vals

    @property
    def _col_meta(self) -> ColumnMetadataABC:
        return self.__col_meta

    @property
    def _row(self):
        return self.__row
