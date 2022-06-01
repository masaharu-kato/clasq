"""
    Sample DB definition
"""

from datetime import datetime
from typing import Literal as Lt

from clasq.data.database import DatabaseClass
from clasq.data.table_record import TableClass, ColumnDef
from clasq.schema.sqltypes import VarChar


class SampleDB(DatabaseClass):
    """ Sample DB """
    _db_name = 'testdb'


class Category(TableClass, db=SampleDB, name='categories'):
    name: ColumnDef[VarChar[Lt[64]]]

class Product(TableClass, db=SampleDB, name='products'):
    category: Category
    name: ColumnDef[VarChar[Lt[128]]]
    price: ColumnDef[int]

class User(TableClass, db=SampleDB, name='users'):
    name: ColumnDef[VarChar[Lt[64]]]
    registered_at: ColumnDef[datetime] = None  # type: ignore

class UserSale(TableClass, db=SampleDB, name='user_sales'):
    user: User
    sale_at: ColumnDef[datetime] = None  # type: ignore

class UserSaleProduct(TableClass, db=SampleDB, name='user_sale_products'):
    user_sale: UserSale
    product: Product
    price: ColumnDef[int]
    count: ColumnDef[int]
