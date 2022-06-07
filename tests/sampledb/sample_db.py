"""
    Sample DB definition
"""
from typing import Literal as Lt

from clasq.data.database import DatabaseClass
from clasq.data.table_record import TableClass
from clasq.schema.column import AutoIncrementPrimaryTableColumn, PrimaryTableColumn, TableColumn
from clasq.syntax.data_types import DateTime, Int, Nullable, VarChar


class SampleDB(DatabaseClass):
    """ Sample DB """
    _db_name = 'testdb'


class Category(TableClass, db=SampleDB, name='categories'):
    id: PrimaryTableColumn[Int]
    name: TableColumn[VarChar[Lt[64]]]

class Product(TableClass, db=SampleDB, name='products'):
    id: AutoIncrementPrimaryTableColumn[Int]
    category: Category
    name: TableColumn[VarChar[Lt[128]]]
    price: TableColumn[Int]

class User(TableClass, db=SampleDB, name='users'):
    id: AutoIncrementPrimaryTableColumn[Int]
    name: TableColumn[VarChar[Lt[64]]]
    registered_at: TableColumn[Nullable[DateTime]]

class UserSale(TableClass, db=SampleDB, name='user_sales'):
    id: AutoIncrementPrimaryTableColumn[Int]
    user: User
    sale_at: TableColumn[Nullable[DateTime]]

class UserSaleProduct(TableClass, db=SampleDB, name='user_sale_products'):
    id: AutoIncrementPrimaryTableColumn[Int]
    user_sale: UserSale
    product: Product
    price: TableColumn[Int]
    count: TableColumn[Int]
