"""
    Sample DB definition
"""
from typing import Literal as Lt

from clasq.data.database import DatabaseClass
from clasq.data.table_record import TableClass
from clasq.schema.column import TableColumn
from clasq.syntax.data_types import DateTime, Int, Nullable, VarChar


class SampleDB(DatabaseClass):
    """ Sample DB """
    _db_name = 'testdb'


class Category(TableClass, db=SampleDB, name='categories'):
    name: TableColumn[VarChar[Lt[64]]]

class Product(TableClass, db=SampleDB, name='products'):
    category: Category
    name: TableColumn[VarChar[Lt[128]]]
    price: TableColumn[Int]

class User(TableClass, db=SampleDB, name='users'):
    name: TableColumn[VarChar[Lt[64]]]
    registered_at: TableColumn[Nullable[DateTime]]

class UserSale(TableClass, db=SampleDB, name='user_sales'):
    user: User
    sale_at: TableColumn[Nullable[DateTime]]

class UserSaleProduct(TableClass, db=SampleDB, name='user_sale_products'):
    user_sale: UserSale
    product: Product
    price: TableColumn[Int]
    count: TableColumn[Int]
