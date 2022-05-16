"""
    Test create from class
"""

from datetime import datetime

import pytest
from libsql.syntax.sqltypes import VarChar

class Category:
    name: VarChar[64]

class Product:
    category: Category
    name: VarChar[128]
    price: int

class User:
    name: VarChar[64]
    registered_at: datetime

class UserSale:
    user: User
    datetime: datetime

class UserSaleProduct:
    user_sale: UserSale
    product: Product
    price: int
    count: int


@pytest.mark.parametrize(('cls', 'query'), [
    (Category, 
        b'CREATE TABLE `category`('
        b'`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT, '
        b'`name` VARCHAR(64) NOT NULL)'
    ),
    (Product, 
        b'CREATE TABLE `products`('
        b'`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT, '
        b'`category_id` INT NOT NULL, '
        b'`name` VARCHAR(64) NOT NULL, '
        b'`price` INT)'
    ),
    (User,
        b'CREATE TABLE `users`('
        b'`id` INT NOT NULL PRIMARY KEY, '
        b'`name` VARCHAR(64) NOT NULL, '
        b'`registered_at` DATETIME)'
    ),
    (UserSale, 
        b'CREATE TABLE `user_sales`('
        b'`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT, '
        b'`user_id` INT NOT NULL, '
        b'`datetime` DATETIME)'
    ),
    (UserSaleProduct, 
        b'CREATE TABLE `user_sale_products`('
        b'`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT, '
        b'`user_sale_id` INT NOT NULL, '
        b'`product_id` INT NOT NULL, '
        b'`price` INT NOT NULL, '
        b'`count` INT NOT NULL)'
    ),
])

def test_create_table_from_class(cls, query):
    ...
