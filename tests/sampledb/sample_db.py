"""
    Sample DB definition
"""
from typing import Literal as Lt

from clasq.data.database import DatabaseClass
from clasq.data.table_record import TableClass
from clasq.schema.column import AutoIncrementPrimaryTableColumn, PrimaryTableColumn, TableColumn
from clasq.syntax.data_types import DateTime, Int, Nullable, VarChar


class Category(TableClass):
    id: PrimaryTableColumn[Int]
    name: TableColumn[VarChar[Lt[64]]]

class Product(TableClass):
    id: AutoIncrementPrimaryTableColumn[Int]
    category: Category
    name: TableColumn[VarChar[Lt[128]]]
    price: TableColumn[Int]

class User(TableClass):
    id: AutoIncrementPrimaryTableColumn[Int]
    name: TableColumn[VarChar[Lt[64]]]
    registered_at: TableColumn[Nullable[DateTime]]

class UserSale(TableClass):
    id: AutoIncrementPrimaryTableColumn[Int]
    user: User
    sale_at: TableColumn[Nullable[DateTime]]

class UserSaleProduct(TableClass):
    id: AutoIncrementPrimaryTableColumn[Int]
    user_sale: UserSale
    product: Product
    price: TableColumn[Int]
    count: TableColumn[Int]


class SampleDB(DatabaseClass):
    """ Sample DB """
    _db_name = 'testdb'
    categories: Category
    products: Product
    users: User
    user_sales: UserSale
    user_sale_products: UserSaleProduct
