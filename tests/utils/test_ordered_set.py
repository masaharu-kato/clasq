"""
    Test Ordered Set
"""
import pytest

from libsql.utils.ordered_set import FrozenOrderedSet, OrderedSet

ARGS_INIT = [
    [[], []],
    [(), []],
    [[1], [1]],
    [[1, 2], [1, 2]],
    [[1, 1], [1]],
    [[1, 1, 1], [1]],
    [[1, 1, 3, 1], [1, 3]],
    [[1, 1, 3, 3, 1], [1, 3]],
    [[1, 1, 3, 1, 1, 3, 3, 1], [1, 3]],
    [[3, 1, 1, 1, 1, 3, 3, 1], [3, 1]],
    [[17, 5, -10, 3, 2104, 35], [17, 5, -10, 3, 2104, 35]],
    [[17, 5, -10, 3, 3, 2104, 35], [17, 5, -10, 3, 2104, 35]],
    [[17, 5, -10, 35, 2104, 35], [17, 5, -10, 35, 2104]],
    [[39, 39, -10, 35, 2104], [39, -10, 35, 2104]],
    [[39, -10, 35, 39, 2104], [39, -10, 35, 2104]],
    [[39, -10, 35, 39, -10], [39, -10, 35]],
    [[39, -10, 35, 39, 45, 39, -10], [39, -10, 35, 45]],
    [[12.3, 456.79, 3254.64, 534.25, 456.79, 12.3, 3254.24], [12.3, 456.79, 3254.64, 534.25, 3254.24]],
    [['hoge', 'fu', 'ga', 'ga', 'foo', 'fu', 'piyo'], ['hoge', 'fu', 'ga', 'foo', 'piyo']],
    [(39, -10, 35, 39, 2104), [39, -10, 35, 2104]],
    [(39, -10, 35, 39, -10), [39, -10, 35]],
    [(39, -10, 35, 39, 45, 39, -10), [39, -10, 35, 45]],
    [(12.3, 456.79, 3254.64, 534.25, 456.79, 12.3, 3254.24), [12.3, 456.79, 3254.64, 534.25, 3254.24]],
    [('hoge', 'fu', 'ga', 'ga', 'foo', 'fu', 'piyo'), ['hoge', 'fu', 'ga', 'foo', 'piyo']],
]

@pytest.mark.parametrize('args, result', ARGS_INIT)
def test_init_f(args, result):
    assert list(FrozenOrderedSet(args)) == result

@pytest.mark.parametrize('args, result', ARGS_INIT)
def test_init_n(args, result):
    assert list(OrderedSet(args)) == result

@pytest.mark.parametrize('args, result', ARGS_INIT)
def test_init_nf(args, result):
    assert list(OrderedSet(args)) == list(FrozenOrderedSet(args))

@pytest.mark.parametrize('args, result', ARGS_INIT)
def test_init_fn(args, result):
    assert list(FrozenOrderedSet(args)) == list(OrderedSet(args))


ARGS_AND = [
    [[], [], []],
    [[1], [], []],
    [[], [1], []],
    [(), [], []],
    [[], (), []],
    [(), (), []],
    [[1], (), []],
    [(), [1], []],
    [(1,), [], []],
    [[], (1,), []],
    [[1], [1], [1]],
    [[1], [2], []],
    [[1, 2], [1], [1]],
    [[1], [2, 1], [1]],
    [[1, 2, 3], [2, 3], [2, 3]],
    [[1, 2, 3], [3, 2], [2, 3]],
    [[1, 3, 2, 4, 5, 1], [5, 3, 1, 2], [1, 3, 2, 5]],
    [[1, 2, 2], [1], [1]],
    [[1, 2, 2], [2, 9, 9, 13], [2]],
    [[10, 19, 19, 24, 24, 21, 10], [9, 4, 3, 10, 10, 3, 19], [10, 19]],
    [(1, 2, 3), [3, 2], [2, 3]],
    [(1, 3, 2, 4, 5, 1), (5, 3, 1, 2), [1, 3, 2, 5]],
    [[1, 2, 2], (1, ), [1]],
    [(1, 2, 2), [2, 9, 9, 13], [2]],
    [[10, 19, 19, -24, 24, 21, 10], (9, 4, 3, 10, 10, 3, 19), [10, 19]],
    [['hoge', 'fuga', 'piyopiyo'], ['piyopiyo', 'hoger', 'fuga'], ['fuga', 'piyopiyo']],
    [('hoge', 'fuga', 'piyopiyo'), ['piyopiyo', 'hoger', 'fuga'], ['fuga', 'piyopiyo']],
    [['hoge', 'fuga', 'piyopiyo'], ('piyopiyo', 'hoger', 'fuga'), ['fuga', 'piyopiyo']],
    [('hoge', 'fuga', ''), ('piyopiyo', '', 'fuga'), ['fuga', '']],
]

@pytest.mark.parametrize('arg1, arg2, result', ARGS_AND)
def test_and_ff(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1) & FrozenOrderedSet(arg2)) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_AND)
def test_and_fn(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1) & OrderedSet(arg2)) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_AND)
def test_and_nf(arg1, arg2, result):
    assert list(OrderedSet(arg1) & FrozenOrderedSet(arg2)) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_AND)
def test_and_nn(arg1, arg2, result):
    assert list(OrderedSet(arg1) & OrderedSet(arg2)) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_AND)
def test_intersection_ff(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1).intersection(FrozenOrderedSet(arg2))) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_AND)
def test_intersection_fn(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1).intersection(OrderedSet(arg2))) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_AND)
def test_intersection_nf(arg1, arg2, result):
    assert list(OrderedSet(arg1).intersection(FrozenOrderedSet(arg2))) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_AND)
def test_intersection_nn(arg1, arg2, result):
    assert list(OrderedSet(arg1).intersection(OrderedSet(arg2))) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_AND)
def test_iand_f(arg1, arg2, result):
    oset = OrderedSet(arg1)
    oset &= FrozenOrderedSet(arg2)
    assert list(oset) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_AND)
def test_iand_n(arg1, arg2, result):
    oset = OrderedSet(arg1)
    oset &= OrderedSet(arg2)
    assert list(oset) == result


ARGS_OR = [
    [[], [], []],
    [[1], [], [1]],
    [[], [1], [1]],
    [(), [], []],
    [[], (), []],
    [(), (), []],
    [[1], (), [1]],
    [(), [1], [1]],
    [(1,), [], [1]],
    [[], (1,), [1]],
    [[1], [1], [1]],
    [[1], [2], [1, 2]],
    [[1, 1], [1], [1]],
    [[1], [2, 1], [1, 2]],
    [[1, 2, 3], [2, 3], [1, 2, 3]],
    [[1, 2, 3], [3, 2], [1, 2, 3]],
    [[1, 3, 2, 4, 5, 1], [5, 3, 1, 2], [1, 3, 2, 4, 5]],
    [[1, 2, 2], [1], [1, 2]],
    [[1, 2, 2], [2, 9, 9, 13], [1, 2, 9, 13]],
    [[10, 19, 19, -24, -24, 21, 10], [9, 4, 3, 10, 10, 3, 19], [10, 19, -24, 21, 9, 4, 3]],
    [(10, 19, 19, -24, -24, 21, 10), [9, 4, 3, 10, 10, 3, 19], [10, 19, -24, 21, 9, 4, 3]],
    [[10, 19, 19, -24, -24, 21, 10], (9, 4, 3, 10, 10, 3, 19), [10, 19, -24, 21, 9, 4, 3]],
    [(10, 19, 19, -24, -24, 21, 10), (9, 4, 3, 10, 10, 3, 19), [10, 19, -24, 21, 9, 4, 3]],
    [['hoge', 'fuga', 'piyopiyo'], ['piyopiyo', 'hoger', 'fuga'], ['hoge', 'fuga', 'piyopiyo', 'hoger']],
    [('hoge', 'oo', 'piyopiyo'), ['piyopiyo', 'hoger', 'fuga'], ['hoge', 'oo', 'piyopiyo', 'hoger', 'fuga']],
    [['hoge', 'fuga', ''], ('piyopiyo', '', 'fuga'), ['hoge', 'fuga', '', 'piyopiyo']],
]

@pytest.mark.parametrize('arg1, arg2, result', ARGS_OR)
def test_or_ff(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1) | FrozenOrderedSet(arg2)) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_OR)
def test_or_fn(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1) | OrderedSet(arg2)) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_OR)
def test_or_nf(arg1, arg2, result):
    assert list(OrderedSet(arg1) | FrozenOrderedSet(arg2)) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_OR)
def test_or_nn(arg1, arg2, result):
    assert list(OrderedSet(arg1) | OrderedSet(arg2)) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_OR)
def test_union_ff(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1).union(FrozenOrderedSet(arg2))) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_OR)
def test_union_fn(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1).union(OrderedSet(arg2))) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_OR)
def test_union_nf(arg1, arg2, result):
    assert list(OrderedSet(arg1).union(FrozenOrderedSet(arg2))) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_OR)
def test_union_nn(arg1, arg2, result):
    assert list(OrderedSet(arg1).union(OrderedSet(arg2))) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_OR)
def test_ior_f(arg1, arg2, result):
    oset = OrderedSet(arg1)
    oset |= FrozenOrderedSet(arg2)
    assert list(oset) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_OR)
def test_ior_n(arg1, arg2, result):
    oset = OrderedSet(arg1)
    oset |= OrderedSet(arg2)
    assert list(oset) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_OR)
def test_update_iterable(arg1, arg2, result) -> None:
    oset = OrderedSet(arg1)
    oset.update(arg2)
    assert list(oset) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_OR)
def test_update_vals(arg1, arg2, result) -> None:
    oset = OrderedSet(arg1)
    oset.update(*((arg,) for arg in arg2))
    assert list(oset) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_OR)
def test_update_n(arg1, arg2, result) -> None:
    oset = OrderedSet(arg1)
    oset.update(OrderedSet(arg2))
    assert list(oset) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_OR)
def test_update_f(arg1, arg2, result) -> None:
    oset = OrderedSet(arg1)
    oset.update(FrozenOrderedSet(arg2))
    assert list(oset) == result


ARGS_SUB = [
    [[], [], []],
    [[1], [], [1]],
    [[], [1], []],
    [(), [], []],
    [[], (), []],
    [(), (), []],
    [[1], (), [1]],
    [(), [1], []],
    [(1,), [], [1]],
    [[], (1,), []],
    [[1], [1], []],
    [[1], [2], [1]],
    [[1, 2], [1], [2]],
    [[1], [2, 1], []],
    [[1, 2, 3], [2, 3], [1]],
    [[1, 2, 4, 3], [3, 2], [1, 4]],
    [[1, 3, 2, 4, 5, 1], [5, 3, 1, 2], [4]],
    [[1, 2, 2], [1], [2]],
    [[2, 13, 14, 9, 1, 1, 2], [2, 9, 9, 13], [14, 1]],
    [[10, 19, 19, -24, -24, 21, 10], [9, 4, 3, 10, 10, 3, 19], [-24, 21]],
    [(10, 19, 19, -24, -24, 21, 10), [9, 4, 3, 10, 10, 3, 19], [-24, 21]],
    [[10, 19, 19, -24, -24, 21, 10], (9, 4, 3, 10, 10, 3, 19), [-24, 21]],
    [(10, 19, 19, -24, -24, 21, 10), (9, 4, 3, 10, 10, 3, 19), [-24, 21]],
    [['hoge', 'fuga', 'piyopiyo'], ['piyopiyo', 'hoger', 'fuga'], ['hoge']],
    [('hoge', 'fuga', 'piyopiyo'), ['piyo', 'hoger', 'piyo', 'fuga'], ['hoge', 'piyopiyo']],
    [['hoge', 'fuga', 'fuga', 'piyopiyo'], ('piyopiyo', 'hoger', 'fuga'), ['hoge']],
    [('hoge', 'fuga', '', 'hoge', 'hoge'), ('piyopiyo', '', 'fga'), ['hoge', 'fuga']],
]

@pytest.mark.parametrize('arg1, arg2, result', ARGS_SUB)
def test_sub_ff(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1) - FrozenOrderedSet(arg2)) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_SUB)
def test_sub_fn(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1) - OrderedSet(arg2)) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_SUB)
def test_sub_nf(arg1, arg2, result):
    assert list(OrderedSet(arg1) - FrozenOrderedSet(arg2)) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_SUB)
def test_sub_nn(arg1, arg2, result):
    assert list(OrderedSet(arg1) - OrderedSet(arg2)) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_SUB)
def test_difference_ff(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1).difference(FrozenOrderedSet(arg2))) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_SUB)
def test_difference_fn(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1).difference(OrderedSet(arg2))) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_SUB)
def test_difference_nf(arg1, arg2, result):
    assert list(OrderedSet(arg1).difference(FrozenOrderedSet(arg2))) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_SUB)
def test_difference_nn(arg1, arg2, result):
    assert list(OrderedSet(arg1).difference(OrderedSet(arg2))) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_SUB)
def test_isub_f(arg1, arg2, result):
    oset = OrderedSet(arg1)
    oset -= FrozenOrderedSet(arg2)
    assert list(oset) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_SUB)
def test_isub_n(arg1, arg2, result):
    oset = OrderedSet(arg1)
    oset -= OrderedSet(arg2)
    assert list(oset) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_SUB)
def test_difference_update_f(arg1, arg2, result):
    oset = OrderedSet(arg1)
    oset.difference_update(FrozenOrderedSet(arg2))
    assert list(oset) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_SUB)
def test_difference_update_n(arg1, arg2, result):
    oset = OrderedSet(arg1)
    oset.difference_update(OrderedSet(arg2))
    assert list(oset) == result


ARGS_XOR = [
    [[], [], []],
    [[1], [], [1]],
    [[], [1], [1]],
    [(), [], []],
    [[], (), []],
    [(), (), []],
    [[1], (), [1]],
    [(), [1], [1]],
    [(1,), [], [1]],
    [[], (1,), [1]],
    [[1], [1], []],
    [[1], [2], [1, 2]],
    [[1, 2], [1], [2]],
    [[1], [2, 1], [2]],
    [[1, 2, 3], [2, 3], [1]],
    [[1, 2, 4, 3], [3, 2], [1, 4]],
    [[1, 3, 2, 4, 5, 1], [6, 3, 1, 2], [4, 5, 6]],
    [[1, 2, 2], [1], [2]],
    [[2, 13, 14, 9, 1, 1, 2], [2, 9, 16, 9, 13, 17], [14, 1, 16, 17]],
    [[10, 19, 19, -24, -24, 21, 10], [9, 4, 3, 10, 10, 3, 19], [-24, 21, 9, 4, 3]],
    [(10, 19, 19, -24, -24, 21, 10), [9, 4, 3, 10, 10, 3, 19], [-24, 21, 9, 4, 3]],
    [[10, 19, 19, -24, -24, 21, 10], (9, 4, 3, 10, 10, 3, 19), [-24, 21, 9, 4, 3]],
    [(10, 19, 19, -24, -24, 21, 10), (9, 4, 3, 10, 10, 3, 19), [-24, 21, 9, 4, 3]],
    [['hoge', 'fuga', 'piyopiyo'], ['piyopiyo', 'hoger', 'fuga'], ['hoge', 'hoger']],
    [('hoge', 'fuga', 'piyopiyo'), ['piyo', 'hoger', 'piyo', 'fuga'], ['hoge', 'piyopiyo', 'piyo', 'hoger']],
    [['hoge', 'fuga', 'fuga', 'piyopiyo'], ('piyopiyo', 'hoger', 'fuga'), ['hoge', 'hoger']],
    [('hoge', 'fuga', '', 'hoge', 'hoge'), ('piyopiyo', '', 'fga'), ['hoge', 'fuga', 'piyopiyo', 'fga']],
]

@pytest.mark.parametrize('arg1, arg2, result', ARGS_XOR)
def test_xor_ff(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1) ^ FrozenOrderedSet(arg2)) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_XOR)
def test_xor_fn(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1) ^ OrderedSet(arg2)) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_XOR)
def test_xor_nf(arg1, arg2, result):
    assert list(OrderedSet(arg1) ^ FrozenOrderedSet(arg2)) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_XOR)
def test_xor_nn(arg1, arg2, result):
    assert list(OrderedSet(arg1) ^ OrderedSet(arg2)) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_XOR)
def test_symmetric_difference_ff(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1).symmetric_difference(FrozenOrderedSet(arg2))) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_XOR)
def test_symmetric_difference_fn(arg1, arg2, result):
    assert list(FrozenOrderedSet(arg1).symmetric_difference(OrderedSet(arg2))) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_XOR)
def test_symmetric_difference_nf(arg1, arg2, result):
    assert list(OrderedSet(arg1).symmetric_difference(FrozenOrderedSet(arg2))) == result
    
@pytest.mark.parametrize('arg1, arg2, result', ARGS_XOR)
def test_symmetric_difference_nn(arg1, arg2, result):
    assert list(OrderedSet(arg1).symmetric_difference(OrderedSet(arg2))) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_XOR)
def test_ixor_f(arg1, arg2, result):
    oset = OrderedSet(arg1)
    oset ^= FrozenOrderedSet(arg2)
    assert list(oset) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_XOR)
def test_ixor_n(arg1, arg2, result):
    oset = OrderedSet(arg1)
    oset ^= OrderedSet(arg2)
    assert list(oset) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_XOR)
def test_symmetric_difference_f(arg1, arg2, result):
    oset = OrderedSet(arg1)
    oset.symmetric_difference_update(FrozenOrderedSet(arg2))
    assert list(oset) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_XOR)
def test_symmetric_difference_n(arg1, arg2, result):
    oset = OrderedSet(arg1)
    oset.symmetric_difference_update(OrderedSet(arg2))
    assert list(oset) == result


ARGS_CONTAIN = [
    [[], 1, False],
    [(), 1, False],
    [[1], 1, True],
    [[1], 2, False],
    [[1, 2], 1, True],
    [[1, 2, 3], 2, True],
    [[1, -3, 4, 4, -3], -3, True],
    [[1, 3, 2, 4, 5, 1], 0, False],
    [[1, 0, 0],0, True],
    [[2, 13, 14, 9, 1, 1, 2], 13, True],
    [[10, 19, 19, 24, 24, 21, 10], 25, False],
    [[12.3, 456.79, 1.5, 534.25, 456.79, 12.3, 1.5], 1.5, True],
    [[12.3, 456.79, 1.5, 534.25, 456.79, 12.3, 1.5], 1.6, False],
    [['hoge', 'fu', 'ga', 'ga', 'foo', 'fu', 'piyo'], 'g', False],
    [['hoge', 'fu', 'ga', 'ga', 'foo', 'fu', 'piyo'], 'piyo', True],
    [['hoge', 'hoge', 'hoge'], 'hoge', True],
    [(2, 13, 14, 9, 1, 1, 2), 13, True],
    [(10, 19, 19, 24, 24, 21, 10), 25, False],
    [(12.3, 456.79, 1.5, 534.25, 456.79, 12.3, 1.5), 1.6, False],
    [('hoge', 'fu', 'ga', 'ga', 'foo', 'fu', 'piyo'), 'piyo', True],
    [('hoge', 'hoge', 'hoge'), 'hoge', True],
]

@pytest.mark.parametrize('arg1, arg2, result', ARGS_CONTAIN)
def test_contains_f(arg1, arg2, result):
    assert (arg2 in FrozenOrderedSet(arg1)) == result

@pytest.mark.parametrize('arg1, arg2, result', ARGS_CONTAIN)
def test_contains_n(arg1, arg2, result):
    assert (arg2 in OrderedSet(arg1)) == result


ARGS_LEN = [
    [[], 0],
    [(), 0],
    [[1], 1],
    [[1, 2], 2],
    [[1, 3, 4, 4, 3], 3],
    [[2, 13, -14, 9, 1, 1, 2], 5],
    [(2, 13, 14, 9, 1, -1, 2), 6],
    [[12.3, 456.79, 1.5, 534.25, 456.79, 12.3, 1.5], 4],
    [['hoge', 'fu', 'ga', 'ga', 'foo', 'fu', 'piyo'], 5],
    [('hoge', 'hoge', 'hoge'), 1],
]

@pytest.mark.parametrize('arg, result', ARGS_LEN)
def test_len_f(arg, result):
    assert len(FrozenOrderedSet(arg)) == result

@pytest.mark.parametrize('arg, result', ARGS_LEN)
def test_len_n(arg, result):
    assert len(OrderedSet(arg)) == result

@pytest.mark.parametrize('arg, result', ARGS_LEN)
def test_bool_f(arg, result):
    assert bool(FrozenOrderedSet(arg)) == bool(result)

@pytest.mark.parametrize('arg, result', ARGS_LEN)
def test_bool_n(arg, result):
    assert bool(OrderedSet(arg)) == bool(result)

    
@pytest.mark.parametrize('arg, val, result', [
    [[], 12, [12]],
    [(), 24, [24]],
    [[12], 12, [12]],
    [[12], 15, [12, 15]],
    [[1, 2], 2, [1, 2]],
    [[1, 2, 1], 1, [1, 2]],
    [[1, 9], 2, [1, 9, 2]],
    [[1, 3, 4, 4, 3], 5, [1, 3, 4, 5]],
    [[2, 13, 14, 9, 1, 1, 2], 0, [2, 13, 14, 9, 1, 0]],
    [(2, 13, 14, 9, 1, 1, 2), 0, [2, 13, 14, 9, 1, 0]],
    [[12.3, 456.79, 1.5, 534.25, 456.79, 12.3, 1.5], -2.5, [12.3, 456.79, 1.5, 534.25, -2.5]],
    [['hoge', 'fu', 'ga', 'ga', 'foo', 'fu', 'piyo'], 'ga', ['hoge', 'fu', 'ga', 'foo', 'piyo']],
    [('hoge', 'hoge', 'hoge'), 'fuga', ['hoge', 'fuga']],
    [('hoge', 'hoge', 'hoge'), '', ['hoge', '']],
])
def test_add(arg, val, result) -> None:
    oset = OrderedSet(arg)
    oset.add(val)
    assert list(oset) == result


@pytest.mark.parametrize('arg, val, result', [
    [[], 12, []],
    [(), 24, []],
    [[12], 12, []],
    [[12], 15, [12]],
    [[1, 2], 2, [1]],
    [[1, 2, 1], 1, [2]],
    [[1, 9], 2, [1, 9]],
    [[1, 3, 4, 4, 3], 4, [1, 3]],
    [[2, 13, 14, 9, 1, 1, 2], 0, [2, 13, 14, 9, 1]],
    [(2, 13, 14, 9, 1, 1, 2), 0, [2, 13, 14, 9, 1]],
    [[12.3, 456.79, 1.5, 534.25, 456.79, 12.3, 1.5], 1.5, [12.3, 456.79, 534.25]],
    [['hoge', 'fu', 'ga', 'ga', 'foo', 'fu', 'piyo'], 'ga', ['hoge', 'fu', 'foo', 'piyo']],
    [('hoge', 'hoge', '', 'hoge'), 'hoge', ['']],
    [('hoge', 'hoge', '', 'hoge'), '', ['hoge']],
])
def test_discard(arg, val, result) -> None:
    oset = OrderedSet(arg)
    oset.discard(val)
    assert list(oset) == result


@pytest.mark.parametrize('arg, val, result', [
    [[12], 12, []],
    [[1, 2], 2, [1]],
    [[1, 2, 1], 1, [2]],
    [[1, 3, 4, 4, 3], 4, [1, 3]],
    [(1, 3, 4, 4, 3), 4, [1, 3]],
    [[12.3, 456.79, 1.5, 534.25, 456.79, 12.3, 1.5], 1.5, [12.3, 456.79, 534.25]],
    [['hoge', 'fu', 'ga', 'ga', 'foo', 'fu', 'piyo'], 'ga', ['hoge', 'fu', 'foo', 'piyo']],
    [('hoge', 'hoge', '', 'hoge'), 'hoge', ['']],
    [('hoge', 'hoge', '', 'hoge'), '', ['hoge']],
])
def test_remove(arg, val, result) -> None:
    oset = OrderedSet(arg)
    oset.remove(val)
    assert list(oset) == result

@pytest.mark.parametrize('arg', [
    [],
    (),
    [1],
    [1, 2],
    [5, 2, 2, 1, 3, 5, 4],
    (5, 2, 2, 1, 3, 5, 4),
    [12.3, 456.79, 1.5, 534.25, 456.79, 12.3, 1.5],
    ['hoge', 'fuga', '', ''],
])
def test_clear(arg) -> None:
    oset = OrderedSet(arg)
    oset.clear()
    assert not list(oset)


# TODO: Add test_le
# TODO: Add test_lt
# TODO: Add test_ge
# TODO: Add test_gt
# TODO: Add test_pop
# TODO: Add test_pop_first
