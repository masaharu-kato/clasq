"""
    Test Ordered Set
"""
import pytest

from libsql.utils.ordered_set import OrderedSet

@pytest.mark.parametrize('args, result', [
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
])
def test_init(args, result):
    assert list(OrderedSet(args)) == result


@pytest.mark.parametrize('arg1, arg2, result', [
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
    [[10, 19, 19, 24, 24, 21, 10], (9, 4, 3, 10, 10, 3, 19), [10, 19]],
    [['hoge', 'fuga', 'piyopiyo'], ['piyopiyo', 'hoger', 'fuga'], ['fuga', 'piyopiyo']],
    [('hoge', 'fuga', 'piyopiyo'), ['piyopiyo', 'hoger', 'fuga'], ['fuga', 'piyopiyo']],
    [['hoge', 'fuga', 'piyopiyo'], ('piyopiyo', 'hoger', 'fuga'), ['fuga', 'piyopiyo']],
    [('hoge', 'fuga', ''), ('piyopiyo', '', 'fuga'), ['fuga', '']],
])
def test_and(arg1, arg2, result):
    assert list(OrderedSet(arg1) & OrderedSet(arg2)) == result


@pytest.mark.parametrize('arg1, arg2, result', [
    [[1], [1], [1]],
    [[1], [2], [1, 2]],
    [[1, 1], [1], [1]],
    [[1], [2, 1], [1, 2]],
    [[1, 2, 3], [2, 3], [1, 2, 3]],
    [[1, 2, 3], [3, 2], [1, 2, 3]],
    [[1, 3, 2, 4, 5, 1], [5, 3, 1, 2], [1, 3, 2, 4, 5]],
    [[1, 2, 2], [1], [1, 2]],
    [[1, 2, 2], [2, 9, 9, 13], [1, 2, 9, 13]],
    [[10, 19, 19, 24, 24, 21, 10], [9, 4, 3, 10, 10, 3, 19], [10, 19, 24, 21, 9, 4, 3]],
    [['hoge', 'fuga', 'piyopiyo'], ['piyopiyo', 'hoger', 'fuga'], ['hoge', 'fuga', 'piyopiyo', 'hoger']],
    [('hoge', 'oo', 'piyopiyo'), ['piyopiyo', 'hoger', 'fuga'], ['hoge', 'oo', 'piyopiyo', 'hoger', 'fuga']],
    [['hoge', 'fuga', ''], ('piyopiyo', '', 'fuga'), ['hoge', 'fuga', '', 'piyopiyo']],
])
def test_or(arg1, arg2, result):
    assert list(OrderedSet(arg1) | OrderedSet(arg2)) == result


@pytest.mark.parametrize('arg1, arg2, result', [
    [[1], [1], []],
    [[1], [2], [1]],
    [[1, 2], [1], [2]],
    [[1], [2, 1], []],
    [[1, 2, 3], [2, 3], [1]],
    [[1, 2, 4, 3], [3, 2], [1, 4]],
    [[1, 3, 2, 4, 5, 1], [5, 3, 1, 2], [4]],
    [[1, 2, 2], [1], [2]],
    [[2, 13, 14, 9, 1, 1, 2], [2, 9, 9, 13], [14, 1]],
    [[10, 19, 19, 24, 24, 21, 10], [9, 4, 3, 10, 10, 3, 19], [24, 21]],
    [['hoge', 'fuga', 'piyopiyo'], ['piyopiyo', 'hoger', 'fuga'], ['hoge']],
    [('hoge', 'fuga', 'piyopiyo'), ['piyo', 'hoger', 'piyo', 'fuga'], ['hoge', 'piyopiyo']],
    [['hoge', 'fuga', 'fuga', 'piyopiyo'], ('piyopiyo', 'hoger', 'fuga'), ['hoge']],
    [('hoge', 'fuga', '', 'hoge', 'hoge'), ('piyopiyo', '', 'fga'), ['hoge', 'fuga']],
])
def test_sub(arg1, arg2, result):
    assert list(OrderedSet(arg1) - OrderedSet(arg2)) == result


@pytest.mark.parametrize('arg1, arg2, result', [
    [[1], 1, True],
    [[1], 2, False],
    [[1, 2], 1, True],
    [[1, 2, 3], 2, True],
    [[1, 3, 4, 4, 3], 3, True],
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
])
def test_contains(arg1, arg2, result):
    assert (arg2 in OrderedSet(arg1)) == result
