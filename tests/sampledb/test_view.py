"""
    Test View object
"""
import pytest
from clasq.connection import MySQLConnection
from clasq.schema.column import NamedViewColumnABC, TableColumn
from clasq.syntax.exprs import QueryArg
from clasq.syntax.abc.keywords import OrderType
from clasq.utils.tabledata import TableData
from clasq.errors import QueryArgumentError

def test_view_1():
    db = MySQLConnection(user='testuser', password='testpass', database='testdb').db

    categories = db['categories']
    assert [str(c.name) for c in categories._selected_exprs] == ['id', 'name']

    cate_name_col = categories['name']
    assert isinstance(cate_name_col, NamedViewColumnABC) 
    assert str(cate_name_col.name) == 'name'
    assert isinstance(cate_name_col, TableColumn) 
    assert cate_name_col.table == categories

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

    assert keyboards.result == products.where(category_id=4).result

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
        id = db['products']['id'],
        category_id = db['products']['category_id'],
        category_name = db['categories']['name'],
        name = db['products']['name'],
        price = db['products']['price'],
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


def test_view_2():
    db = MySQLConnection(user='testuser', password='testpass', database='testdb').db
    cates, prods, sales = db['categories', 'products', 'user_sale_products']

    comp_cates = cates.where((cates['id'] == 1) | (cates['id'] == 2))
    comps = prods.inner_join(comp_cates, comp_cates['id'] == prods['category_id'])
    comps.result
    assert comps.result[2]['price'] == 89000


def test_view_3():
    db = MySQLConnection(user='testuser', password='testpass', database='testdb').db
    cates, prods, sales = db['categories', 'products', 'user_sale_products']

    sales_count = sales['count'].sum().as_('sales_count')
    sales_price = (sales['count'] * sales['price']).sum().as_('sales_price')

    base_view = (prods
        .inner_join(
            cates.select_column(cate_id=cates['id'], cate_name=cates['name']),
            prods['category_id'] == cates['id']
        )
        .left_join(sales, sales['product_id'] == prods['id'])
        .group_by(prods['id'])
        .add_column(sales_count, sales_price)
        .order_by(-sales_price, -sales_count, -prods['price'])
    )

    assert len(base_view) == 30
    assert base_view.result[7]['name'] == 'Gaming Mouse'

    filtered_view = base_view.where(sales_price >= 5000)


def test_view_with_args():
    db = MySQLConnection(user='testuser', password='testpass', database='testdb').db
    cates, prods, sales = db['categories', 'products', 'user_sale_products']

    view = (prods
        .inner_join(cates.select_column(category_name = cates['name']),
            prods['category_id'] == cates['id'])
        .where(cates['id'] == QueryArg(0))
        .order_by(-prods['price'])
    )

    with pytest.raises(QueryArgumentError):
        view.prepare_result()

    res_argval_2 = view.with_args(2).result
    assert len(res_argval_2) == 3
    assert res_argval_2[1]['price'] == 79000

    with pytest.raises(QueryArgumentError):
        view.with_args(1, 2).result

    view2 = (prods
        .inner_join(cates.select_column(category_name = cates['name']),
            prods['category_id'] == cates['id'])
        .where(cates['name'].like(QueryArg('cate_name_like')))
        .order_by(-prods['price'])
    )

    res_argval_comp = view2(cate_name_like='%Computer')
    assert len(res_argval_comp) == 9
    assert res_argval_comp[7]['name'] == 'Notebook CPU 2cores, RAM 4GB, SSD 128GB'

    res_argval_laptop = view2(cate_name_like='Laptop%')
    assert res_argval_2 == res_argval_laptop

    with pytest.raises(QueryArgumentError):
        view2('%Computer')

    view3 = view2.limit(QueryArg('limit', default=5))

    assert len(view3(cate_name_like='%Computer')) == 5
    assert len(view3(cate_name_like='%Computer', limit=3)) == 3

    with pytest.raises(QueryArgumentError):
        view3(limit=3)

    with pytest.raises(QueryArgumentError):
        view4 = (prods
            .inner_join(cates.select_column(category_name = cates['name']),
                prods['category_id'] == cates['id'])
            .where(name=QueryArg('name'))
            .where(category_name=QueryArg('name', default='Cables'))
            .order_by(-prods['price'])
        )
        view4(name='Cables')


# def test_view_4():
#     db = clasq.mysql.connect(user='testuser', password='testpass', database='testdb')
#     cates, prods, sales = db['categories', 'products', 'user_sale_products']

#     base_view = (prods
#         .inner_join(cates, prods['category_id'] == cates['id'])
#         .left_join(sales, sales['product_id'] == prods['id'])
#         .group_by(prods['id'])
#     )
#     base_view = base_view.select_column(
#         cate_id=cates['id'],
#         cate_name=cates['name'],
#         id=prods['id'],
#         name=prods['name'],
#         price=prods['price'],
#         sales_count=sales['count'].sum(),
#         sales_price=(sales['count'] * sales['price']).sum(),
#     )
#     base_view = base_view.order_by(sales_price=False, sales_count=False, price=False)

#     base_view.self_based_view().where(base_view['sales_price'] >= 5000).query_select


def main():
    test_view_with_args()

if __name__ == '__main__':
    main()

