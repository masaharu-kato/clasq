"""
    Test View object
"""
import libsql
from libsql.utils.tabledata import TableData
from libsql.syntax.keywords import OrderType

def test_view_1():
    db = libsql.mysql.connect(user='testuser', password='testpass', database='testdb')

    categories = db['categories']
    assert categories.result == TableData(
        ['id', 'name'], [
            (1, 'Desktop Computer'),
            (2, 'Laptop Computer'),
            (3, 'Display'),
            (4, 'Keyboard'),
            (5, 'Mouse'),
            (6, 'Cables'),
        ]
    )

    ordered_categories = categories.order_by(categories['name'])
    assert ordered_categories.result == TableData(
        ['id', 'name'], [
            (6, 'Cables'),
            (1, 'Desktop Computer'),
            (3, 'Display'),
            (4, 'Keyboard'),
            (2, 'Laptop Computer'),
            (5, 'Mouse'),
        ]
    )

    assert categories.order_by(name=True) == ordered_categories.result
    assert categories.order_by(name='ASC') == ordered_categories.result
    assert categories.order_by(name=OrderType.ASC) == ordered_categories.result


    products = db['products']

    keyboards = products.where(products['category_id'] == 4)
    assert keyboards.result == TableData(
        ['id', 'category_id', 'name', 'price'], [
            (17, 4, 'Lowcost Keyboard', 2000),
            (18, 4, 'Silver Keyboard', 9000),
            (19, 4, 'Red Keyboard', 7000),
            (20, 4, 'Blue Keyboard', 8000),
            (21, 4, 'Wireless Keyboard', 5000),
        ]
    )

    ordered_keyboards = keyboards.order_by(-products['price'])  # (-keyboards['price'])
    assert ordered_keyboards.result == TableData(
        ['id', 'category_id', 'name', 'price'], [
            (18, 4, 'Silver Keyboard', 9000),
            (20, 4, 'Blue Keyboard', 8000),
            (19, 4, 'Red Keyboard', 7000),
            (21, 4, 'Wireless Keyboard', 5000),
            (17, 4, 'Lowcost Keyboard', 2000),
        ]
    )


    joined_view = products\
    .inner_join(db['categories'], products['category_id'] == db['categories']['id'])\
    .select_column(
        db['products']['id'],
        db['products']['category_id'],
        db['categories']['name'].as_('category_name'),
        db['products']['name'],
        db['products']['price'],
    )\
    .where(db['categories']['id'].in_(3, 4))\
    .order_by(-db['products']['price'])\
    [:10]

    assert joined_view.result == TableData(
        ['id', 'category_id', 'category_name', 'name', 'price'], [
            (16, 3, 'Display', 'Display 30-inch 4K', 50000),
            (15, 3, 'Display', 'Display 28-inch 4K', 40000),
            (13, 3, 'Display', 'Display 28-inch Full-HD', 32000),
            (12, 3, 'Display', 'Display 27-inch Full-HD', 30000),
            (14, 3, 'Display', 'Display 26-inch 4K', 30000),
            (11, 3, 'Display', 'Display 26-inch Full-HD', 25000),
            (10, 3, 'Display', 'Display 24-inch Full-HD', 20000),
            (18, 4, 'Keyboard', 'Silver Keyboard', 9000),
            (20, 4, 'Keyboard', 'Blue Keyboard', 8000),
            (19, 4, 'Keyboard', 'Red Keyboard', 7000),
        ]
    )

