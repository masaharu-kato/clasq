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


def test_basic_select():
    # Normal selection
    db = MySQLConnection(user='testuser', password='testpass', database='testdb')

    products = db['products']
    all_products = db.select(products)
    assert db.last_qd == QueryData(b'SELECT `products`.* FROM `products`')
    assert len(all_products) == 30
    assert all_products[0]['id'] == 1 and all_products[0]['price'] == 60000

    # Where 
    computers = db.select(products, where=products['category_id'] == 1)
    true_qd = QueryData(b'SELECT `products`.* FROM `products` WHERE (`products`.`category_id` = ?)', prms=[1])
    print(db.last_qd)
    print(true_qd)
    assert db.last_qd == true_qd
    assert len(computers) == 6
    assert computers[0]['id'] == 1 and computers[0]['price'] == 60000

    # Order (one column)
    sorted_computers = db.select(products, where=products['category_id'] == 1, orders=[-products['price']])
    assert db.last_qd == QueryData(b'SELECT `products`.* FROM `products` WHERE (`products`.`category_id` = ?) ORDER BY `products`.`price` DESC', prms=[1])
    assert len(sorted_computers) == 6
    assert sorted_computers[0]['id'] == 4 and sorted_computers[0]['price'] == 140000

    # # Order (multiple column)
    # displays = db.select(products, where=products['category_id'] == 3



