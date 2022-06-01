"""
    Table data class
"""
from __future__ import annotations
from abc import ABC, abstractmethod, abstractproperty
import html
from typing import Any, Callable, Collection, Generic, Iterable, Iterator, Mapping, Sequence, TypeVar, overload

T = TypeVar('T')

class ColumnMetadataABC(ABC, Collection[str]):

    @abstractproperty
    def columns(self) -> tuple[str, ...]:
        """ Get a tuple of all column names

        Returns:
            Tuple[str]: Tuple of all column names
        """

    @abstractproperty
    def _col_to_i(self) -> dict[str, int]:
        """ Get a dictionary from column name to column index """

    def iter_columns(self) -> Iterator[str]:
        """ Iterate column names

        Yields:
            Iterator[str]: Colum names
        """
        return iter(self.columns)

    def __iter__(self):
        """ Iterate column names
            (Synonym of `iter_columns`)

        Yields:
            Iterator[str]: Colum names
        """
        return self.iter_columns()

    def __len__(self) -> int:
        return len(self.columns)

    def column_to_i(self, column: str) -> int:
        """ Get the index corresponding to the speficied column name

        Args:
            column (str): Column name to get the index

        Returns:
            int: Column index (from 0)
        """
        return self._col_to_i[column]

    def has_column(self, column: str) -> bool:
        """ Check if the specified name of column exists on this table

        Args:
            column (str): Column name to check existence

        Returns:
            bool: Column existence
        """
        return column in self._col_to_i

    def __contains__(self, column: object) -> bool:
        """ Check if the specified name of column exists on this table
            (Synonym of `has_column`)

        Args:
            column (str): Column name to check existence

        Returns:
            bool: Column existence
        """
        return isinstance(column, str) and self.has_column(column)

    def make_html(self, *, name_attr: str | None = None) -> str:
        """ Make a HTML of this column header

        Args:
            name_attr (str | None, optional): HTML tag attribute name to output column name. Defaults to None.

        Returns:
            str: HTML markup text
        """
        if name_attr:
            _cell_html = lambda c: '<TH %s="%s">%s</TH>' % (name_attr, _escape_attr(c), _escape_val(c))
        else:
            _cell_html = lambda c: '<TH>%s</TH>' % _escape_val(c)
        return '<TR>' + ''.join(_cell_html(v) for v in self.iter_columns()) + '</TR>'

    def __eq__(self, value) -> bool:
        """ Check if all columns are equal to another column metadata

        Args:
            value : Another column metadata

        Returns:
            bool: Equal or not
        """
        if not isinstance(value, ColumnMetadataABC):
            return NotImplemented
        col_meta = value
        return self.columns == col_meta.columns


class RowDataABC(Generic[T], Mapping[str, T]):

    @abstractproperty
    def _col_meta(self) -> ColumnMetadataABC:
        """ Get a columns meta data """

    @abstractproperty
    def _row(self) -> tuple[T, ...]:
        """ Get a raw row tuple """

    def __getitem__(self, val: int | str) -> T:
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

    def __iter__(self) -> Iterator[str]:
        return iter(self.columns)

    def __len__(self) -> int:
        return len(self.columns)

    _T = TypeVar('_T')
    @overload
    def get(self, key: str) -> T | None: ...

    @overload
    def get(self, key: str, default: _T) -> T | _T: ...

    def get(self, key: str, default: _T | None = None):
        """ Get a value of specific column with a default value

        Args:
            column (int | str): Index or Name of the column to get a value
            default (Any, optional): Default value
                used if the specific column is not found. Defaults to None.

        Returns:
            Any: Value of the specified column, or a default value
        """
        # if isinstance(val, int):
        #     return self._row[val] if val >= 0 and val < len(self._row) else default
        return self[key] if key in self._col_meta else default

    @property
    def columns(self):
        return self._col_meta.columns

    @property
    def raw_values(self):
        return self._row

    def asdict(self) -> dict[str, T]:
        """ Make a dictionary from column names to cell values of this row

        Returns:
            dict[str, T]: Dict of (column name -> cell values) of this row
        """
        return dict(self.items())

    def make_html(self, *, name_attr: str | None = None, formatter: Callable[[Any], str] = str) -> str:
        """ Make a HTML of this row

        Args:
            name_attr (str | None, optional): HTML tag attribute name to output column name. Defaults to None.
            formatter (Callable[[Any], str], optional): Cell value format function. Defaults to str.

        Returns:
            str: HTML markup text
        """
        if name_attr:
            _cell_html = lambda k, v: '<TD %s="%s">%s</TD>' % (name_attr, _escape_attr(k), _escape_val(formatter(v)))
        else:
            _cell_html = lambda k, v: '<TD>%s</TD>' % _escape_val(formatter(v))
        return '<TR>' + ''.join(_cell_html(k, v) for k, v in self.items()) + '</TR>'

    def __eq__(self, value) -> bool:
        """ Check if all columns and values are equal to another row data

        Args:
            value : Another row data

        Returns:
            bool: Equal or not
        """
        if not isinstance(value, RowDataABC):
            return NotImplemented
        row_data = value
        return self._col_meta == row_data._col_meta and self._row == row_data._row



class FrozenTableDataABC(ABC, Sequence[RowDataABC[T]], Generic[T]):
    """ Table data object """

    TABLE_REPR_LIMIT = 100

    @abstractproperty
    def rows_values(self) -> Sequence[tuple[T, ...]]:
        """ Get rows data """

    @abstractproperty
    def col_meta(self) -> ColumnMetadataABC:
        """ Get a column metadata """

    @abstractmethod
    def _new_row_data(self, col_meta: ColumnMetadataABC, row: tuple[T, ...]) -> RowDataABC[T]:
        """ Create a new RowDataABC object """

    def __iter__(self) -> Iterator[RowDataABC[T]]:
        """ Iterate rows

        Yields:
            RowData: Row data class
        """
        for row in self.rows_values:
            yield self._new_row_data(self.col_meta, row)

    @property
    def columns(self) -> tuple[str, ...]:
        """ Get a tuple of all column names of this table

        Returns:
            Tuple[str]: All column names of this table
        """
        return self.col_meta.columns

    def iter_columns(self) -> Iterator[str]:
        """ Iterate column names of this table

        Yields:
            Iterator[str]: colum names of this table
        """
        return self.col_meta.iter_columns()

    def iter_rows_values(self) -> Iterator[tuple[T, ...]]:
        """ Iterate values of all rows in this table

        Yields:
            Iterator[tuple[T, ...]]: Values of rows in this table
        """
        return iter(self.rows_values)

    def iter_rows_dict(self) -> Iterator[dict[str, T]]:
        """ Iterate values of all dicts (column name -> cell value) in this table

        Yields:
            Iterator[tuple[T, ...]]: Dicts of rows in this table
        """
        for row in iter(self):
            yield dict(row.items())

    def rows_values_list(self) -> list[tuple[T, ...]]:
        """ Get a list of all row values

        Returns:
            List[tuple[T, ...]]: List of all row values
        """
        return list(self.iter_rows_values())

    def rows_dict_list(self) -> list[dict[str, T]]:
        """ Get a list of all row dicts (column name -> cell value)

        Returns:
            List[dict[str, T]]: List of all row dicts
        """
        return list(self.iter_rows_dict())

    def __len__(self) -> int:
        """ Get a number of rows in this table

        Returns:
            int: A number of rows in this table
        """
        return len(self.rows_values)
    
    def row(self, index: int) -> RowDataABC[T]:
        """ Get a speicific row data object

        Args:
            index (int): Row index (from 0) to get

        Returns:
            RowData[T]: Row data object
        """
        return self._new_row_data(self.col_meta, self.row_values(index))

    def row_values(self, index: int) -> tuple[T, ...]:
        return self.rows_values[index]

    @overload
    def __getitem__(self, index: int) -> RowDataABC[T]: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[RowDataABC[T]]: ...

    def __getitem__(self, index: int | slice):
        """ Get a speicific row data object
            (Synonym of `row` method)

        Args:
            index (int): Row index (from 0) to get

        Returns:
            RowData[T]: Row data object
        """
        if isinstance(index, slice):
            raise NotImplementedError()
        if not isinstance(index, int):
            raise TypeError('Invalid index type', index)
        return self.row(index)

    def __eq__(self, val: object) -> bool:
        """ Compare with another TableData object
            Check if all columns are same and all values in tables are equal.

        Args:
            val (TableData): Another TableData object

        Returns:
            bool: All columns are same and values in tables are equal, or not
        """
        if not isinstance(val, FrozenTableDataABC):
            return super().__eq__(val)
        return self.col_meta == val.col_meta and self.rows_values == val.rows_values

    def make_html(self, *, name_attr: str | None = None, formatter: Callable[[Any], str] = str) -> str:
        """ Make a HTML of this table

        Args:
            name_attr (str | None, optional): HTML tag attribute name to output column name. Defaults to None.
            formatter (Callable[[Any], str], optional): Cell values format function. Defaults to str.

        Returns:
            str: HTML markup text
        """
        return ('<TABLE>\n'
            + self.col_meta.make_html(name_attr=name_attr) + '\n'
            + '\n'.join(row.make_html(name_attr=name_attr, formatter=formatter) for row in self)
            + '\n</TABLE>'
        )

    def __repr__(self) -> str:
        res = 'TableData#%d\n' % id(self)
        # res += '\t'.join(str(c) for c in self._col_meta.iter_columns()) + '\n'
        # for i, row in enumerate(iter(self)):
        #     if i > TABLE_REPR_LIMIT:
        #         break
        #     res += '\t'.join(str(v) for v in row) + '\n'
        return res

    def _repr_html_(self) -> str:
        """ Make a HTML of this table
            (For IPython environment)

        Returns:
            str: HTML markup text
        """
        return self.make_html()


class TableDataABC(FrozenTableDataABC[T], Generic[T]):
    """ Table Data abstract class 
        (Writable)
    """

    @abstractproperty
    def rows_values_list(self) -> list[tuple[T, ...]]:
        """ Extend another rows to the existing rows """

    def append(self, value: tuple[T, ...] | TableDataABC) -> None:
        """ Append another TableData

        Args:
            value (tuple[T, ...] | TableDataABC): row values or TableData to append

        Raises:
            ValueError: Columns of tables are different
        """
        if isinstance(value, TableDataABC):
            table_data = value
            if not (self.col_meta == table_data.col_meta):
                raise ValueError('Cannot combine table data with different columns.')
            return self.extend(table_data.rows_values)

        if len(value) != len(self.col_meta):
            raise ValueError('Cannot combine table data with different columns.')
        
        self.rows_values_list.append(value)

    def __iadd__(self, value: tuple[T, ...] | TableDataABC) -> TableDataABC:
        """ Append another TableData
            (Synonym of `append` method)

        Args:
            value (tuple[T, ...] | TableDataABC): value to append

        Raises:
            ValueError: Columns of tables are different
            TypeError: Type of value is invalid
        """
        if not isinstance(value, (TableDataABC, RowDataABC)):
            return NotImplemented
        self.append(value)
        return self

    def extend(self, rows: Iterable[tuple[T, ...]]) -> None:
        self.rows_values_list.extend(rows)

    def pop(self, index: int | None = None) -> RowDataABC[T]:
        _raw_values = self.rows_values_list.pop(index) if index is not None else self.rows_values_list.pop()
        return self._new_row_data(self.col_meta, _raw_values)
    
    def insert(self, index: int, target: tuple[T, ...]) -> None:
        self.rows_values_list.insert(index, target)

    def clear(self) -> None:
        self.rows_values_list.clear()




def _escape_attr(val):
    return html.escape(val, quote=True)

def _escape_val(val):
    return html.escape(val)
