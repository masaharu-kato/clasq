import pytest
from libsql.connector import MySQLConnection
from libsql.executor import QueryExecutor
from dbconfig import DBCFG

USERS = [
    {'id': 1, 'role_id':1, 'username':'Default User' , 'age':20},
    {'id': 2, 'role_id':2, 'username':'Administrator', 'age':30},
]
 


@pytest.mark.parametrize('username', ['admin', 'loader', 'viewer']) 
def test_connection(username):
    cnx = MySQLConnection(
        host = DBCFG['server']['host'],
        port = DBCFG['server']['port'],
        user = DBCFG['users'][username]['username'],
        password = DBCFG['users'][username]['password'],
        database = DBCFG['server']['databasename'],
    )

    assert cnx

    cursor = cnx.create_cursor(dictionary=True)
    cursor.execute('SELECT * FROM users ORDER BY id LIMIT 2')
    assert cursor.fetchall() == USERS

    with QueryExecutor(cursor) as executor:
        assert executor.query('SELECT * FROM users WHERE id = 1') == USERS[:1]
        assert executor.query_one('SELECT * FROM users WHERE id = 1') == USERS[0]

    cursor.close()

    with pytest.raises(TypeError):
        executor = QueryExecutor(cnx)

    with cnx.executor(dictionary=True) as executor:
        assert executor.query('SELECT * FROM users WHERE id = 1') == USERS[:1]
        assert executor.query_one('SELECT * FROM users WHERE id = 1') == USERS[0]


def test_dict_connection():
    cnx = MySQLConnection(
        host = DBCFG['server']['host'],
        port = DBCFG['server']['port'],
        user = DBCFG['users']['viewer']['username'],
        password = DBCFG['users']['viewer']['password'],
        database = DBCFG['server']['databasename'],
        dictionary=True
    )

    assert cnx

    cursor = cnx.create_cursor()
    cursor.execute('SELECT * FROM users ORDER BY id LIMIT 2')
    assert cursor.fetchall() == USERS

    with cnx.executor() as executor:
        assert executor.query('SELECT * FROM users WHERE id = 1') == USERS[:1]
        assert executor.query_one('SELECT * FROM users WHERE id = 1') == USERS[0]
