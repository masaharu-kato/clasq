"""
    Test Query Data
"""

from math import ceil, floor, trunc
import pytest

from libsql.syntax.schema_expr import ObjectExpr as Obj
from libsql.syntax.query_data import QueryData

@pytest.mark.parametrize('term, result', [
    [Obj(b'expr') == None , (b'(`expr` = ?)' , [None])],
    [Obj(b'expr') == True , (b'(`expr` = ?)' , [True])],
    [Obj(b'expr') == False, (b'(`expr` = ?)' , [False])],
    [Obj(b'expr') == 1    , (b'(`expr` = ?)' , [1])],
    [Obj(b'expr') == 12.45, (b'(`expr` = ?)' , [12.45])],
    [Obj(b'expr') == 'val', (b'(`expr` = ?)' , ['val'])],
    [Obj(b'expr') +  123,   (b'(`expr` + ?)' , [123])],
    [Obj(b'expr') -  123,   (b'(`expr` - ?)' , [123])],
    [Obj(b'expr') *  123,   (b'(`expr` * ?)' , [123])],
    [Obj(b'expr') /  123,   (b'(`expr` / ?)' , [123])],
    [Obj(b'expr') %  123,   (b'(`expr` % ?)' , [123])],
    [Obj(b'expr') == 123,   (b'(`expr` = ?)', [123])],
    [Obj(b'expr') != 123,   (b'(`expr` != ?)', [123])],
    [Obj(b'expr') >= 123,   (b'(`expr` >= ?)', [123])],
    [Obj(b'expr') >  123,   (b'(`expr` > ?)' , [123])],
    [Obj(b'expr') <= 123,   (b'(`expr` <= ?)', [123])],
    [Obj(b'expr') <  123,   (b'(`expr` < ?)' , [123])],
    [Obj(b'expr') // 123,   (b'(`expr` DIV ?)', [123])],
    [Obj(b'abc') & Obj(b'defg'), (b'(`abc` AND `defg`)', [])],
    [Obj(b'abc') | Obj(b'defg'), (b'(`abc` OR `defg`)', [])],
    [Obj(b'abc') ^ Obj(b'defg'), (b'(`abc` XOR `defg`)', [])],
    [(Obj(b'abc') == 123) & (Obj(b'defg') == 456), (b'((`abc` = ?) AND (`defg` = ?))', [123, 456])],
    [(Obj(b'abc') == 123) | (Obj(b'defg') == 456), (b'((`abc` = ?) OR (`defg` = ?))', [123, 456])],
    [(Obj(b'abc') == 123) ^ (Obj(b'defg') == 456), (b'((`abc` = ?) XOR (`defg` = ?))', [123, 456])],
    [Obj(b'expr1') + Obj(b'expr2') >= 1000, (b'((`expr1` + `expr2`) >= ?)' , [1000])],
    [(Obj(b'abc') > 123) | ((Obj(b'abc') == 123) & (Obj(b'defg') >= 456)), (b'((`abc` > ?) OR ((`abc` = ?) AND (`defg` >= ?)))', [123, 123, 456])],
    [+Obj(b'expr'), (b'`expr`', [])],
    [-Obj(b'expr'), (b'- `expr`', [])],
    [abs  (Obj(b'expr')), (b'ABS(`expr`)', [])],
    [ceil (Obj(b'expr')), (b'CEIL(`expr`)', [])],
    [floor(Obj(b'expr')), (b'FLOOR(`expr`)', [])],
    [trunc(Obj(b'expr')), (b'TRUNCATE(`expr`)', [])],
])
def test_expr_op(term, result):
    qd = QueryData(term)
    true_stmt, true_prms = result
    assert qd.stmt == true_stmt and qd.prms == true_prms


@pytest.mark.parametrize('args, result', [
    [(), (b'', [])],
    [(None,), (b'', [])],
    [(True,), (b'?', [True])],
    [(False,), (b'?', [False])],
    [(1,), (b'?', [1])],
    [(123.45,), (b'?', [123.45])],
    [('text',), (b'?', ['text'])],
    [(b'text',), (b'text', [])],
    [(Obj(b'text'),), (b'`text`', [])],
    [(Obj(b'text') == 123,), (b'(`text` = ?)', [123])],
    [([1],), (b'?', [1])],
    [([1, 2, 5],), (b'?, ?, ?', [1, 2, 5])],
    [(b'abc', b'DEFG', b'HI', b'jkl',), (b'abc DEFG HI jkl', [])],
    [([b'abc', b'DEFG', b'HI', b'jkl'],), (b'abc, DEFG, HI, jkl', [])],
    [(b'hoge', b'fugar'), (b'hoge fugar', [])],
    [(b'hoge', b'fugar', b'(', b')'), (b'hoge fugar ()', [])],
    [(b'hoge', b'fugar', b'(', 123, b')'), (b'hoge fugar (?)', [123])],
    [(b'hoge', b'fugar(', 123, b')'), (b'hoge fugar(?)', [123])],
    [(b'hoge', b'fugar(', 123, 'textval', b')'), (b'hoge fugar(? ?)', [123, 'textval'])],
    [(b'hoge', b'fugar(', 123, 'textval', b')', b'foo'), (b'hoge fugar(? ?) foo', [123, 'textval'])],
    [(b'hoge', b'fugar(', Obj(b'name') == 'textval', b')', b'foo'), (b'hoge fugar((`name` = ?)) foo', ['textval'])],
    [(b'hoge', b'fugar(', [123, 'textval'], b')', b'foo'), (b'hoge fugar(?, ?) foo', [123, 'textval'])],
])
def test_query_data(args, result):
    qd = QueryData(*args)
    true_stmt, true_prms = result
    assert qd.stmt == true_stmt and qd.prms == true_prms

