"""
    Table data class
"""
from hashlib import new
import html
from typing import Any, Callable, Dict, Generic, Iterable, Iterator, List, Optional, Tuple, TypeVar, Union

T = TypeVar('T')

class TableData(Generic[T]):
    """ Table data object """

    TABLE_REPR_LIMIT = 100

    def __init__(self, columns: Union[Iterable[str], 'ColumnMetadata'], rows: List[Tuple[T]]) -> None:
        """ Create a table data.

        Args:
            columns (List[str]): List of column names
            rows (List[list]): List of rows (values of columns)
        """
        self._col_meta = columns if isinstance(columns, ColumnMetadata) else ColumnMetadata(columns)
        
        if not (isinstance(rows, list) and all(isinstance(row, tuple) for row in rows)):
            raise TypeError('Invalid type of arguments.')

        self._rows = rows

    def __iter__(self) -> Iterator['RowData[T]']:
        """ Iterate rows

        Yields:
            RowData: Row data class
        """
        for row in self._rows:
            yield RowData(self._col_meta, row)

    def __add__(self, value: 'TableData') -> 'TableData':
        """ Get a new TableData with another TableData

        Args:
            value (TableData): Another TableData object

        Raises:
            ValueError: Columns of tables are different

        Returns:
            TableData: A new TableData object
        """
        if not isinstance(value, TableData):
            return NotImplemented
        table_data = value
        if not (self._col_meta == table_data._col_meta):
            raise ValueError('Cannot combine table data with different columns.')
        return TableData(self._col_meta, [*self._rows, *table_data._rows])

    def append(self, table_data: 'TableData') -> 'TableData':
        """ Append another TableData

        Args:
            value (TableData): TableData to append

        Raises:
            ValueError: Columns of tables are different

        Returns:
            TableData: Self object
        """
        if not (self._col_meta == table_data._col_meta):
            raise ValueError('Cannot combine table data with different columns.')
        self._rows.extend(table_data._rows)
        return self

    def __iadd__(self, value: 'TableData') -> 'TableData':
        """ Append another TableData
            (Synonym of `append` method)

        Args:
            value (TableData): TableData to append

        Raises:
            ValueError: Columns of tables are different

        Returns:
            TableData: Self object
        """
        if not isinstance(value, TableData):
            return NotImplemented
        return self.append(value)
    
    def set_columns(self, new_columns: Iterable[str]) -> None:
        """ Set a new column names

        Args:
            new_columns (Iterable[str]): New column names

        Raises:
            ValueError: Number of columns does not match.
        """
        new_col_meta = ColumnMetadata(new_columns)
        if not (len(self.columns) == len(new_col_meta.columns)):
            raise ValueError('Number of columns does not match.')
        self._col_meta = new_col_meta

    def copy_with_columns(self, new_columns: Iterable[str]) -> 'TableData':
        """ Get a shallow copy of this table with new column names

        Args:
            new_columns (Iterable[str]): New column names

        Returns:
            TableData: New TableData object
        """
        return TableData(new_columns, self._rows)

    @property
    def columns(self) -> Tuple[str, ...]:
        """ Get a tuple of all column names of this table

        Returns:
            Tuple[str]: All column names of this table
        """
        return self._col_meta.columns

    def iter_columns(self) -> Iterator[str]:
        """ Iterate column names of this table

        Yields:
            Iterator[str]: colum names of this table
        """
        return self._col_meta.iter_columns()

    def iter_rows_values(self) -> Iterator[Tuple[T]]:
        """ Iterate values of all rows in this table

        Yields:
            Iterator[Tuple[T]]: Values of rows in this table
        """
        return iter(self._rows)

    def iter_rows_dict(self) -> Iterator[Dict[str, T]]:
        """ Iterate values of all dicts (column name -> cell value) in this table

        Yields:
            Iterator[Tuple[T]]: Dicts of rows in this table
        """
        for row in iter(self):
            yield dict(row.items())

    def rows_values_list(self) -> List[Tuple[T]]:
        """ Get a list of all row values

        Returns:
            List[Tuple[T]]: List of all row values
        """
        return list(self.iter_rows_values())

    def rows_dict_list(self) -> List[Dict[str, T]]:
        """ Get a list of all row dicts (column name -> cell value)

        Returns:
            List[Dict[T]]: List of all row dicts
        """
        return list(self.iter_rows_dict())

    def __len__(self) -> int:
        """ Get a number of rows in this table

        Returns:
            int: A number of rows in this table
        """
        return len(self._rows)
    
    def row(self, index: int) -> 'RowData[T]':
        """ Get a speicific row data object

        Args:
            index (int): Row index (from 0) to get

        Returns:
            RowData[T]: Row data object
        """
        return RowData(self._col_meta, self._rows[index])

    def __getitem__(self, index: int) -> 'RowData[T]':
        """ Get a speicific row data object
            (Synonym of `row` method)

        Args:
            index (int): Row index (from 0) to get

        Returns:
            RowData[T]: Row data object
        """
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
        if not isinstance(val, TableData):
            return NotImplemented
        return self._col_meta == val._col_meta and self._rows == val._rows

    def make_html(self, *, name_attr: Optional[str] = None, formatter: Callable[[Any], str] = str) -> str:
        """ Make a HTML of this table

        Args:
            name_attr (Optional[str], optional): HTML tag attribute name to output column name. Defaults to None.
            formatter (Callable[[Any], str], optional): Cell values format function. Defaults to str.

        Returns:
            str: HTML markup text
        """
        return ('<TABLE>\n'
            + self._col_meta.make_html(name_attr=name_attr) + '\n'
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


class ColumnMetadata:
    def __init__(self, columns: Iterable[str]) -> None:
        """ Create a column metadata.

        Args:
            columns (List[str]): List of column names
        """
        self._cols = tuple(columns)
        self._col_to_i = {col: i for i, col in enumerate(self._cols)}

    @property
    def columns(self) -> Tuple[str, ...]:
        """ Get a tuple of all column names

        Returns:
            Tuple[str]: Tuple of all column names
        """
        return self._cols

    def iter_columns(self) -> Iterator[str]:
        """ Iterate column names

        Yields:
            Iterator[str]: Colum names
        """
        return iter(self._cols)

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

    def make_html(self, *, name_attr: Optional[str] = None) -> str:
        """ Make a HTML of this column header

        Args:
            name_attr (Optional[str], optional): HTML tag attribute name to output column name. Defaults to None.

        Returns:
            str: HTML markup text
        """
        if name_attr:
            _cell_html = lambda c: '<TH %s="%s">%s</TH>' % (name_attr, _escape_attr(c), _escape_val(c))
        else:
            _cell_html = lambda c: '<TH>%s</TH>' % _escape_val(c)
        return '<TR>' + ''.join(_cell_html(v) for v in self.iter_columns()) + '</TR>'

    def __contains__(self, column: str) -> bool:
        """ Check if the specified name of column exists on this table

        Args:
            column (str): Column name to check existence

        Returns:
            bool: Column existence
        """
        return self.has_column(column)

    def __eq__(self, value) -> bool:
        """ Check if all columns are equal to another column metadata

        Args:
            value : Another column metadata

        Returns:
            bool: Equal or not
        """
        if not isinstance(value, ColumnMetadata):
            return NotImplemented
        col_meta = value
        return self._cols == col_meta._cols


class RowData(Generic[T]):
    def __init__(self, column_metadata: ColumnMetadata, row: Tuple[T]) -> None:
        """ Create a row data.

        Args:
            column_metadata (ColumnMetadata): Column metadata.
            row (tuple): Row data (values of columns)
        """
        self._col_meta = column_metadata
        self._row = row

    def __getitem__(self, val: Union[int, str]) -> T:
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

    def get(self, val: Union[int, str], default: Optional[T] = None) -> Optional[T]:
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

    def __iter__(self) -> Iterator[T]:
        """ Iterate row values

        Yields:
            Iterator: row value
        """
        # return (self._row[i] for i in self._col_meta.iter_indexes())
        return iter(self._row)

    def items(self) -> Iterator[Tuple[str, T]]:
        """ Iterate columns with its name and value

        Yields:
            Iterator[Tuple[str, Any]]: Column name and its value
        """
        return zip(self._col_meta.iter_columns(), self._row)

    def make_dict(self) -> Dict[str, T]:
        """ Make a dictionary from column names to cell values of this row

        Returns:
            Dict[str, T]: Dict of (column name -> cell values) of this row
        """
        return dict(self.items())

    def make_html(self, *, name_attr: Optional[str] = None, formatter: Callable[[Any], str] = str) -> str:
        """ Make a HTML of this row

        Args:
            name_attr (Optional[str], optional): HTML tag attribute name to output column name. Defaults to None.
            formatter (Callable[[Any], str], optional): Cell value format function. Defaults to str.

        Returns:
            str: HTML markup text
        """
        if name_attr:
            _cell_html = lambda k, v: '<TD %s="%s">%s</TD>' % (name_attr, _escape_attr(k), _escape_val(formatter(v)))
        else:
            _cell_html = lambda k, v: '<TD>%s</TD>' % _escape_val(formatter(v))
        return '<TR>' + ''.join(_cell_html(k, v) for k, v in self.items()) + '</TR>'

    @property
    def __dict__(self):
        """ Generate a dictionary of this row

        Returns:
            dict: A dictionary of this row
        """
        return self.make_dict()


def _escape_attr(val):
    return html.escape(val, quote=True)

def _escape_val(val):
    return html.escape(val)
