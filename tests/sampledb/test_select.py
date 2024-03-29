"""
    Test basic select
"""
import pytest
import clasq.connection
from clasq.syntax.query_data import QueryData

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
    con = clasq.connection.MySQLConnection(user='testuser', password='testpass', database='testdb')
    db = con.db

    SELECT_PRODUCTS_SQL = b'SELECT `products`.`id`, `products`.`category_id`, `products`.`name`, `products`.`price` FROM `products`'

    products = db['products']
    assert list(products._selected_exprs) == [products['id'], products['category_id'], products['name'], products['price']]

    products.prepare_result()

    assert products._select_query == QueryData(stmt=SELECT_PRODUCTS_SQL)
    assert len(products) == 30
    assert products.result[0]['id'] == 1 and products.result[0]['price'] == 60000

    # Where 
    computers = products.where(products['category_id'] == 1)
    computers.result
    assert computers._select_query == QueryData(stmt=SELECT_PRODUCTS_SQL + b' WHERE (`products`.`category_id` = ?)', prms=[1])
    assert len(computers) == 6
    assert computers.result[0]['id'] == 1 and computers.result[0]['price'] == 60000

    # Order (one column)
    sorted_computers = computers.order_by(-products['price'])
    assert sorted_computers._select_query == QueryData(stmt=SELECT_PRODUCTS_SQL + b' WHERE (`products`.`category_id` = ?) ORDER BY `products`.`price` DESC', prms=[1])
    assert len(sorted_computers) == 6
    assert sorted_computers.result[0]['id'] == 4 and sorted_computers.result[0]['price'] == 140000

    # # Order (multiple column)
    # displays = db.select(products, where=products['category_id'] == 3
    

def test_select():
    
    db = clasq.connection.MySQLConnection(user='testuser', password='testpass', database='testdb').db
    cates, prods, sales = db['categories'], db['products'], db['user_sale_products']

    sales_count = sales['count'].sum().as_('sales_count')
    sales_price = (sales['count'] * sales['price']).sum().as_('sales_price')

    view1 = prods.inner_join(cates, prods['category_id'] == cates['id'])
    view2 = view1.left_join(sales, sales['product_id'] == prods['id'])
    view3 = view2.where(cates['id'].in_(3, 4))
    view = view3.group_by(prods['id'])

    res = view.select_column(
            cates['id'].as_('cate_id'),
            cates['name'].as_('cate_name'),    
            prods['id'],
            prods['name'],
            prods['price'],
            sales_count,
            sales_price
        ).order_by(
            -sales_price,
            -sales_count,
            -prods['price']
        )

    # print(res._select_query)

    assert len(res.result) == 12
    assert str(res.result[0]['sales_price']) == '70000'
    assert str(res.result[1]['sales_price']) == '25000'


def main():
    test_basic_select()
    test_select()

if __name__ == '__main__':
    main()
