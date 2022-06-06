"""
    Test exprs expr
"""
from typing import Callable
import pytest
import datetime
from clasq.syntax.query import QueryData
from clasq.syntax.exprs import BinaryOp, ExprObject as Obj, FuncCall, NoneExpr

CALC_OP_TERMS = [
    [lambda x, y: x +  y, b'+'  ],
    [lambda x, y: x -  y, b'-'  ],
    [lambda x, y: x *  y, b'*'  ],
    [lambda x, y: x /  y, b'/'  ],
    [lambda x, y: x // y, b'DIV'],
    [lambda x, y: x &  y, b'AND'],
    [lambda x, y: x |  y, b'OR' ],
    [lambda x, y: x ^  y, b'XOR'],
]
MOD_OP_TERM = [lambda x, y: x %  y, b'%']

COMP_OP_TERMS = [
    [lambda x, y: x == y, b'='  , b'='],
    [lambda x, y: x != y, b'!=' , b'!='],
    [lambda x, y: x >= y, b'>=' , b'<='],
    [lambda x, y: x >  y, b'>'  , b'<'],
    [lambda x, y: x <= y, b'<=' , b'>='],
    [lambda x, y: x <  y, b'<'  , b'>'],
]

NONSTR_VALUES = [
    123, 45.14, 0,
    datetime.date(2012, 3, 14), datetime.time(21, 5, 39), datetime.datetime(2020, 9, 30, 17, 41, 3)
]

VALUES = ['', 'Hogera', *NONSTR_VALUES]
OBJNAMES = [b'abc', b'bcde', b'z32', b'aabx', b'Fuga', b'pi3yo', b'abc_defg', b'ABC', b'FugaBoo', b'_b3vz', b'abc_']

@pytest.mark.parametrize('term, op', CALC_OP_TERMS)
@pytest.mark.parametrize('x, y', [(Obj(objname), value) for objname, value in zip(OBJNAMES, VALUES)])
def test_calc_expr_obj_val(term: Callable, op: bytes, x: Obj, y):
    assert QueryData(term(x, y)) == QueryData(stmt=b'(`%s` %s ?)' % (x.name, op), prms=[y])
    assert QueryData(term(y, x)) == QueryData(stmt=b'(? %s `%s`)' % (op, x.name), prms=[y])

@pytest.mark.parametrize('term, op', [MOD_OP_TERM])
@pytest.mark.parametrize('x, y', [(Obj(objname), value) for objname, value in zip(OBJNAMES, NONSTR_VALUES)])
def test_calc_mod_expr_obj_val(term: Callable, op: bytes, x: Obj, y):
    assert QueryData(term(x, y)) == QueryData(stmt=b'(`%s` %s ?)' % (x.name, op), prms=[y])
    assert QueryData(term(y, x)) == QueryData(stmt=b'(? %s `%s`)' % (op, x.name), prms=[y])

@pytest.mark.parametrize('term, op, rev_op', COMP_OP_TERMS)
@pytest.mark.parametrize('x, y', [(Obj(objname), value) for objname, value in zip(OBJNAMES, VALUES)])
def test_comp_expr_obj_val(term: Callable, op: bytes, rev_op: bytes, x: Obj, y):
    assert QueryData(term(x, y)) == QueryData(stmt=b'(`%s` %s ?)' % (x.name, op), prms=[y])
    assert QueryData(term(y, x)) == QueryData(stmt=b'(`%s` %s ?)' % (x.name, rev_op), prms=[y])

@pytest.mark.parametrize('term, op', [*CALC_OP_TERMS, *[[term, op] for term, op, _ in COMP_OP_TERMS]])
@pytest.mark.parametrize('x', [Obj(objname) for objname in OBJNAMES[:6]])
@pytest.mark.parametrize('y', [Obj(objname) for objname in OBJNAMES[5:]])
def test_expr_obj_obj(term: Callable, op: bytes, x, y):
    assert QueryData(term(x, y)) == QueryData(stmt=b'(`%s` %s `%s`)' % (x, op, y))
    assert QueryData(term(y, x)) == QueryData(stmt=b'(`%s` %s `%s`)' % (y, op, x))

@pytest.mark.parametrize('term, op', CALC_OP_TERMS)
@pytest.mark.parametrize('x', [Obj(objname) for objname in OBJNAMES])
def test_calc_expr_obj_none_expr(term: Callable, op: bytes, x):
    assert QueryData(term(x, NoneExpr)) == QueryData(x)
    assert QueryData(term(NoneExpr, x)) == QueryData(x)

@pytest.mark.parametrize('term, op', CALC_OP_TERMS)
@pytest.mark.parametrize('x', [Obj(objname) for objname in OBJNAMES[0:3]])
@pytest.mark.parametrize('y', [Obj(objname) for objname in OBJNAMES[3:6]])
@pytest.mark.parametrize('z', [Obj(objname) for objname in OBJNAMES[6:9]])
def test_calc_expr_obj_3(term: Callable, op: bytes, x, y, z):
    term_call = term(x, y)
    assert isinstance(term_call, FuncCall)
    assert isinstance(term_call.func, BinaryOp)
    assert QueryData(term_call.func.call(x, y, z)) == QueryData(stmt=b'(`%s` %s `%s` %s `%s`)' % (x, op, y, op, z))
