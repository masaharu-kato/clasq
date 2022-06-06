"""
    Test create from class
"""
from typing import Type

import pytest

from clasq.data.table_record import TableClass
from sample_db import Category, Product, User, UserSale, UserSaleProduct


@pytest.mark.parametrize(('cls', 'query', 'columns', 'fkeys'), [
    (Category, 
        b'CREATE TABLE `categories` ('
        b'`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT, '
        b'`name` VARCHAR(64) NOT NULL)',
        ['id', 'name'],
        {},
    ),
    (Product, 
        b'CREATE TABLE `products` ('
        b'`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT, '
        b'`category_id` INT NOT NULL, '
        b'`name` VARCHAR(128) NOT NULL, '
        b'`price` INT NOT NULL)',
        ['id', 'category_id', 'name', 'price'],
        {'category_id': 'category'},
    ),
    (User,
        b'CREATE TABLE `users` ('
        b'`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT, '
        b'`name` VARCHAR(64) NOT NULL, '
        b'`registered_at` DATETIME)',
        ['id', 'name', 'registered_at'],
        {},
    ),
    (UserSale, 
        b'CREATE TABLE `user_sales` ('
        b'`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT, '
        b'`user_id` INT NOT NULL, '
        b'`sale_at` DATETIME)',
        ['id', 'user_id', 'sale_at'],
        {},
    ),
    (UserSaleProduct, 
        b'CREATE TABLE `user_sale_products` ('
        b'`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT, '
        b'`user_sale_id` INT NOT NULL, '
        b'`product_id` INT NOT NULL, '
        b'`price` INT NOT NULL, '
        b'`count` INT NOT NULL)',
        ['id', 'user_sale_id', 'product_id', 'price', 'count'],
        {'user_sale_id': 'user_sale', 'product_id': 'product'},
    ),
])

def test_create_table_from_class(cls: Type[TableClass], query, columns: list[str], fkeys: dict[str, str]):
    assert list(str(c.name) for c in cls._dest_table.iter_table_columns()) == columns
    assert cls._dest_table.create_table_query.stmt == query
