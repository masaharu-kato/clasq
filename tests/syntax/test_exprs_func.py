"""
    Test exprs func
"""
import pytest
from libsql.syntax import errors, exprs, object_abc
from libsql.syntax.query_data import QueryData

@pytest.mark.parametrize('funcname, returntype', [
    ('a', None),
    (b'abc', None),
    ('test', None),
    ('testi', int),
    (b'testib', int),
])
def test_no_args_func(funcname, returntype):
    func = exprs.NoArgsFunc(funcname, returntype)
    assert isinstance(func.name, object_abc.ObjectName)
    assert func.name == funcname
    assert func.returntype == returntype

    call = func.call()
    assert isinstance(call, exprs.FuncCall) and call.func == func and not len(call.args)
    assert QueryData(call) == QueryData(stmt=bytes(func.name))
    assert QueryData(call) == QueryData(func())

    with pytest.raises(errors.ObjectArgNumError):
        func.call(1)

    with pytest.raises(errors.ObjectArgNumError):
        func.call(1, 'hoge')

@pytest.mark.parametrize('funcname', ['hoge', 'FUGARE', b'SomeFunc123', b'F1w3Z', b'My_sample_func'])
@pytest.mark.parametrize('args', [
    (),
    (1,),
    (1, 'hoge'),
    ('hoge', 1),
    (1, 'fuga', 3.5, ' ', 0),
])
def test_func(funcname, args):
    func = exprs.Func(funcname)
    assert isinstance(func.name, object_abc.ObjectName)
    assert func.name == funcname

    call = func.call(*args)
    assert isinstance(call, exprs.FuncCall) and call.func == func and call.args == args
    assert QueryData(call) == QueryData(stmt=b'%s(%s)' % (bytes(func.name), b', '.join(b'?' for _ in args)), prms=list(args))
    assert QueryData(call) == QueryData(func(*args))


@pytest.mark.parametrize('funcname', ['hoge', 'FUGARE', b'SomeFunc123'])
@pytest.mark.parametrize('funcargs', [(), (exprs.Expr,), (exprs.Expr, exprs.Expr)])
@pytest.mark.parametrize('args', [
    (),
    (1,),
    (1, 'hoge'),
    ('hoge', 1, 5),
    (1, 'fuga', 3.5, b'piyo', 0),
])
def test_func_with_funcargs(funcname, funcargs, args):
    func = exprs.Func(funcname, [funcargs])
    assert isinstance(func.name, object_abc.ObjectName)
    assert func.name == funcname

    if len(args) == len(funcargs):    
        call = func.call(*args)
        assert isinstance(call, exprs.FuncCall) and call.func == func and call.args == args
        assert QueryData(call) == QueryData(stmt=b'%s(%s)' % (bytes(func.name), b', '.join(b'?' for _ in args)), prms=list(args))
        assert QueryData(call) == QueryData(func(*args))

    else:
        with pytest.raises(errors.ObjectArgNumError):
            func.call(*args)


@pytest.mark.parametrize('funcname', ['hoge', '+', b'*', b'FOO', 'A'])
@pytest.mark.parametrize('arg', [1, 'fuga', 3.5, '', 0])
def test_unary_op(funcname, arg):
    func = exprs.UnaryOp(funcname)
    assert isinstance(func.name, object_abc.ObjectName)
    assert func.name == funcname

    call = func.call(arg)
    assert isinstance(call, exprs.FuncCall) and call.func == func and call.args == (arg, )
    assert QueryData(call) == QueryData(stmt=b'%s ?' % bytes(func.name), prms=[arg])
    assert QueryData(call) == QueryData(func(arg))

    with pytest.raises(errors.ObjectArgNumError):
        func.call()

    with pytest.raises(errors.ObjectArgNumError):
        func.call(arg, 'more')


@pytest.mark.parametrize('funcname', ['plus', '+', b'*', b'AND', '|'])
@pytest.mark.parametrize('arg1', [1, '', 0, 2.5])
@pytest.mark.parametrize('arg2', ['hogfuga', -1.62, 10])
def test_binary_op(funcname, arg1, arg2):
    func = exprs.BinaryOp(funcname)
    assert isinstance(func.name, object_abc.ObjectName)
    assert func.name == funcname

    call_noarg = func()
    assert call_noarg is exprs.NoneExpr

    call_arg1 = func(arg1)
    assert isinstance(call_arg1, exprs.Expr)
    assert call_arg1.v == arg1

    call_arg2 = func.call(arg2)
    assert isinstance(call_arg2, exprs.Expr)
    assert call_arg2.v == arg2

    call = func.call(arg1, arg2)
    assert isinstance(call, exprs.FuncCall) and call.func == func and call.args == (arg1, arg2)
    assert QueryData(call) == QueryData(stmt=b'(? %s ?)' % bytes(func.name), prms=[arg1, arg2])
    assert QueryData(call) == QueryData(func(arg1, arg2))

    call_3 = func.call(arg1, arg2, arg1)
    assert isinstance(call_3, exprs.FuncCall) and call_3.func == func and call_3.args == (arg1, arg2, arg1)
    assert QueryData(call_3) == QueryData(stmt=b'(? %s ? %s ?)' % (bytes(func.name), bytes(func.name)), prms=[arg1, arg2, arg1])
    assert QueryData(call_3) == QueryData(func(arg1, arg2, arg1))
