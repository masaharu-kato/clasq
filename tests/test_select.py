"""
    Test basic select
"""
import libsql
from libsql.syntax.query_data import QueryData

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
    db = libsql.mysql.connect(user='testuser', password='testpass', database='testdb')

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
    

def test_select():
    # Normal selection
    db = libsql.mysql.connect(user='testuser', password='testpass', database='testdb')
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
    assert res[0]['sales_price'] == '70000'
    assert res[1]['sales_price'] == '25000'

