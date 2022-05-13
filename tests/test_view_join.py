"""
    Test View Joins
"""
import libsql
from libsql.schema.column import ViewColumn
from libsql.schema.view import JoinedView
from libsql.syntax.keywords import JoinType

def test_view_join_1():
    db = libsql.mysql.connect(user='testuser', password='testpass', database='testdb')

    products = db['products']
    categories = db['categories']

    view = products.inner_join(categories, products['category_id'] == categories['id'])

    assert isinstance(view, JoinedView)
    assert view.target_view is products
    assert view.join_type == JoinType.INNER
    assert view.view_to_join is categories
    
    JOINED_COLNAMES = ['id', 'category_id', 'name', 'price', 'categories.id', 'categories.name']

    assert all(isinstance(c, ViewColumn) for c in view.columns)
    assert all(c.base_view is view for c in view.columns)
    assert [c.name for c in view.columns] == JOINED_COLNAMES

    assert list(view.columns) == [view[name] for name in JOINED_COLNAMES]


