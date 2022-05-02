"""
    Test basic select
"""

import pytest

from libsql.connection.mysql.connection import MySQLConnection

def test_basic_select():
    db = MySQLConnection(user='testuser', password='testpass', database='testdb')
    db.select(db['books'], where=db['books']['price'] >= 1000)
