"""
    Test basic select (using classes)
"""
import pytest
from clasq.connection import MySQLConnection
from clasq.schema.view import JoinedView
from clasq.syntax.query import QueryData
from sample_db import SampleDB, Category, Product, User, UserSale, UserSaleProduct

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
    SampleDB.connect(MySQLConnection(user='testuser', password='testpass'))

    assert(Product.get_raw_name() == b'products')

    SELECT_PRODUCTS_SQL = b'SELECT `products`.`id`, `products`.`category_id`, `products`.`name`, `products`.`price` FROM `products`'

    assert list(Product._selected_exprs) == [Product.id, Product.category_id, Product.name, Product.price]

    Product.prepare_result()

    assert Product._select_query == QueryData(stmt=SELECT_PRODUCTS_SQL)
    assert len(Product) == 30
    assert Product.result[0]['id'] == 1 and Product.result[0]['price'] == 60000

    # Where 
    computers = Product.where(Product.category_id == 1)
    computers.result
    assert computers._select_query == QueryData(stmt=SELECT_PRODUCTS_SQL + b' WHERE (`products`.`category_id` = ?)', prms=[1])
    assert len(computers) == 6
    assert computers.result[0]['id'] == 1 and computers.result[0]['price'] == 60000

    # Order (one column)
    sorted_computers = computers.order_by(-Product.price)
    assert sorted_computers._select_query == QueryData(stmt=SELECT_PRODUCTS_SQL + b' WHERE (`products`.`category_id` = ?) ORDER BY `products`.`price` DESC', prms=[1])
    assert len(sorted_computers) == 6
    assert sorted_computers.result[0]['id'] == 4 and sorted_computers.result[0]['price'] == 140000

    # # Order (multiple column)
    # displays = db.select(Product, where=Product['category_id'] == 3

    SampleDB.disconnect()
    

def test_select():
    
    SampleDB.connect(MySQLConnection(user='testuser', password='testpass'))

    sales_count = UserSaleProduct.count.sum().as_('sales_count')
    sales_price = (UserSaleProduct.count * UserSaleProduct.price).sum().as_('sales_price')

    view1 = Product.inner_join(Category, Product.category_id == Category.id)
    assert isinstance(view1, JoinedView)
    view2 = view1.left_join(UserSaleProduct, UserSaleProduct.product_id == Product.id)
    view3 = view2.where(Category.id.in_(3, 4))
    view = view3.group_by(Product.id)

    res = view.select_column(
            Category.id.as_('cate_id'),
            Category.name.as_('cate_name'),    
            Product.id,
            Product.name,
            Product.price,
            sales_count,
            sales_price
        ).order_by(
            -sales_price,
            -sales_count,
            -Product.price
        )

    assert len(res.result) == 12
    assert str(res.result[0]['sales_price']) == '70000'
    assert str(res.result[1]['sales_price']) == '25000'

    SampleDB.disconnect()


def main():
    test_basic_select()
    test_select()

if __name__ == '__main__':
    main()

