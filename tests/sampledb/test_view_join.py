"""
    Test View Joins
"""
from libsql.connection.mysql import connect
from libsql.schema.column import NamedViewColumnABC
from libsql.schema.view import JoinedView
from libsql.syntax.exprs import AliasedExpr
from libsql.syntax.keywords import JoinType

def test_view_join_1():
    db = connect(user='testuser', password='testpass', database='testdb')

    products = db['products']
    categories = db['categories']

    view = products.inner_join(categories, products['category_id'] == categories['id'])

    assert isinstance(view, JoinedView)
    assert view.target_view is products
    assert view.join_type == JoinType.INNER
    assert view.view_to_join is categories
    
    JOINED_COLNAMES = ['id', 'category_id', 'name', 'price', 'categories_id', 'categories_name']

    assert all(isinstance(c, (NamedViewColumnABC, AliasedExpr)) for c in view.selected_exprs)
    assert [c.name for c in view.selected_exprs] == JOINED_COLNAMES

    assert list(view.selected_exprs) == [view[name] for name in JOINED_COLNAMES]
