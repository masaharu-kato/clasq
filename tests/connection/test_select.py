"""
    Test basic select
"""
import os

import pytest

from libsql.syntax.query_data import QueryData
from libsql.connection.mysql.connection import MySQLConnection

# # Configure directory
# DIR_CONF = os.path.abspath(os.path.dirname(__file__) + '/../conf')
# assert os.path.exists(DIR_CONF), 'conf directory not exists.'

# # Prepare Database
# db = MySQLConnection(user='testuser', password='testpass', database='testdb')
# with open(DIR_CONF + '/testdb.sql') as f:
#     db.execute_plain(f.read())
# del db


def test_select():
    # Normal selection
    db = MySQLConnection(user='testuser', password='testpass', database='testdb')
    cates, prods, sales = db['categories'], db['products'], db['user_sale_products']

    sales_count = sales['count'].sum().as_('sales_count')
    sales_price = (sales['count'] * sales['price']).sum().as_('sales_price')

    res = db.select(
        cates['id'].as_('cate_id'), cates['name'].as_('cate_name'), prods, sales_count, sales_price,
        joins=[
            (cates, 'INNER', prods['category_id'] == cates['id']),
            (sales, 'LEFT', sales['product_id'] == prods['id']),
        ],
        where=[cates['id'].in_([3, 4])],
        groups=[prods['id']],
        orders=[-sales_price, -sales_count, -prods['price']],
    )

    assert len(res) == 12
    assert res[0]['sales_price'] == 70000
    assert res[1]['sales_price'] == 25000

