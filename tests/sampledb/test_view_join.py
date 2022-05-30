"""
    Test View Joins
"""
from libsql.connection import MySQLConnection
from libsql.schema.column import NamedViewColumnABC
from libsql.schema.view import JoinedView
from libsql.syntax.exprs import AliasedExpr
from libsql.syntax.keywords import JoinType

def test_view_join_1():
    db = MySQLConnection(user='testuser', password='testpass', database='testdb').db

    products = db['products']
    categories = db['categories']

    view = products.inner_join(categories, products['category_id'] == categories['id'])

    assert isinstance(view, JoinedView)
    assert view._target_view is products
    assert view._join_type == JoinType.INNER
    assert view._view_to_join is categories
    
    JOINED_COLNAMES = ['id', 'category_id', 'name', 'price', 'categories_id', 'categories_name']

    assert all(isinstance(c, (NamedViewColumnABC, AliasedExpr)) for c in view._selected_exprs)
    assert [c.name for c in view._selected_exprs] == JOINED_COLNAMES

    assert list(view._selected_exprs) == [view[name] for name in JOINED_COLNAMES]
