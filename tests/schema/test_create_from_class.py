"""
    Test create from class
"""

from datetime import datetime
from typing import Dict, List, Tuple, Type, Literal as Lt

import pytest
from libsql.data.database import DatabaseClass
from libsql.data.table_record import TableClass, ColumnDef
from libsql.schema.sqltype_abc import SQLTypeABC
from libsql.schema.sqltypes import VarChar


class MainDB(DatabaseClass):
    """ Main DB """


class Category(TableClass, MainDB):
    name: ColumnDef[VarChar[Lt[64]]]

class Product(TableClass, MainDB):
    category: Category
    name: ColumnDef[VarChar[Lt[128]]]
    price: ColumnDef[int]

class User(TableClass, MainDB):
    name: ColumnDef[VarChar[Lt[64]]]
    registered_at: ColumnDef[datetime] = None

class UserSale(TableClass, MainDB):
    user: User
    sale_at: ColumnDef[datetime] = None

class UserSaleProduct(TableClass, MainDB):
    user_sale: UserSale
    product: Product
    price: ColumnDef[int]
    count: ColumnDef[int]


@pytest.mark.parametrize(('cls', 'query', 'columns', 'fkeys'), [
    (Category, 
        b'CREATE TABLE `category` ('
        b'`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT, '
        b'`name` VARCHAR(64) NOT NULL)',
        ['id', 'name'],
        {},
    ),
    (Product, 
        b'CREATE TABLE `product` ('
        b'`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT, '
        b'`category_id` INT NOT NULL, '
        b'`name` VARCHAR(128) NOT NULL, '
        b'`price` INT NOT NULL)',
        ['id', 'category_id', 'name', 'price'],
        {'category_id': 'category'},
    ),
    (User,
        b'CREATE TABLE `user` ('
        b'`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT, '
        b'`name` VARCHAR(64) NOT NULL, '
        b'`registered_at` DATETIME DEFAULT NULL)',
        ['id', 'name', 'registered_at'],
        {},
    ),
    (UserSale, 
        b'CREATE TABLE `user_sale` ('
        b'`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT, '
        b'`user_id` INT NOT NULL, '
        b'`sale_at` DATETIME DEFAULT NULL)',
        ['id', 'user_id', 'sale_at'],
        {},
    ),
    (UserSaleProduct, 
        b'CREATE TABLE `user_sale_product` ('
        b'`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT, '
        b'`user_sale_id` INT NOT NULL, '
        b'`product_id` INT NOT NULL, '
        b'`price` INT NOT NULL, '
        b'`count` INT NOT NULL)',
        ['id', 'user_sale_id', 'product_id', 'price', 'count'],
        {'user_sale_id': 'user_sale', 'product_id': 'product'},
    ),
])

def test_create_table_from_class(cls: Type[TableClass], query, columns: List[str], fkeys: Dict[str, str]):
    assert list(str(c.name) for c in cls._table_obj.iter_table_columns()) == columns
    assert cls._table_obj.create_table_query.stmt == query


def main():
    category = Category._table_obj
    print(list(c.name for c in category.iter_table_columns()))
    print(category.create_table_query.stmt)

if __name__ == '__main__':
    main()


