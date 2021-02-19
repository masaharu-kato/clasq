from . import expression
from . import keywords
from abc import abstractmethod
import re
from typing import Any, Callable, List, Sequence, Optional

_IS_DEBUG = True

_HOLDER_STR_ = '%s'

class SQLWithParams:
    """ SQL with parameters (using place-holders) """
    _sql_words: List[str]
    _params: Sequence

    def __init__(self, sql=None, params=None):
        self._sql_words = '' if sql is None else sql
        self._params = [] if params is None else params

    def append(self, val, *vals, raw:bool=False):
        if raw:
            self._sql_words.append(str(val))
        elif callable(val):
            val(self)
        elif isinstance(val, SQLExprType):
            val.__sqlout__(self)
        else:
            self._sql_words.append(_HOLDER_STR_)
            self._params.append(val)

        if vals:
            return self.append(*vals, raw=raw)
        return self

    @property
    def sql(self, minimize:bool=False) -> str:
        if not minimize:
            return ' '.join(self._sql_words)

        sql = ''
        pword:str = '' # None 
        for word in self._sql_words:
            if pword and word and pword[-1].isalpha() and word[0].isalpha():
                sql += ' '
            sql += word
            pword = word

        return sql


    @property
    def params(self) -> Sequence:
        return self._params


    def execute_on(self, func:Callable[[str, Sequence], Any]):
        return func(self.sql, self.params)



SQLWithParamsMaker = Callable[[SQLWithParams], SQLWithParams]



class SQLExprType(expression.ExprABC):
    """ Expression class for SQL """

    def uop(self, op):
        return UnaryExpr(op, self)

    def bop(self, op, y):
        return BinaryExpr(op, self, y)

    def rbop(self, op, y):
        return BinaryExpr(op, y, self)

    def infunc(self, name, *args):
        return FuncExpr(name, self, *args)

    @abstractmethod
    def __sqlout__(self, swp:SQLWithParams) -> None:
        pass


class UnaryExpr(expression.UnaryExpr):
    """ UnaryExpr class for SQL """

    def __sqlout__(self, swp:SQLWithParams) -> None:
        swp.append(_sql_op(self.op), self.x)


class BinaryExpr(expression.BinaryExpr):
    """ UnaryExpr class for SQL """

    def __sqlout__(self, swp:SQLWithParams) -> None:
        swp.append(in_bracket(self.x, _sql_op(self.op), self.y))


class FuncExpr(expression.FuncExpr):
    """ UnaryExpr class for SQL """

    def __sqlout__(self, swp:SQLWithParams) -> None:
        swp.append(_sql_func(self.name), in_bracket(self.args))


def sqlword(word:str) -> SQLWithParamsMaker:
    if not re.match(r'\w+', word):
        raise RuntimeError('Invalid SQL word: `{}`'.format(word))
    return lambda swp: swp.append(word, raw=True)


def sqlobj(name) -> SQLWithParamsMaker:
    objname = str(name)
    if not re.match(r'\w+', name):
        raise RuntimeError('Invalid SQL object name: `{}`'.format(name))
    return lambda swp: swp.append('`' + objname + '`', raw=True)


def in_bracket(*vals) -> SQLWithParamsMaker:
    return lambda swp: swp.append('(', raw=True).append(*vals).append(')', raw=True)

def _joined_maker(swp:SQLWithParamsMaker, *vals, sep:str) -> SQLWithParamsMaker:
    is_first = True
    for val in vals:
        if not is_first:
            swp.append(sep, raw=True)
        is_first = False
        swp.append(val)
    return swp

def joined(*vals, sep:str=',') -> SQLWithParamsMaker:
    return lambda swp: _joined_maker(swp, *vals, sep=sep)

def opt_joined(*vals, sep:str=',') -> Optional[SQLWithParamsMaker]:
    if not vals:
        return None
    return joined(*vals, sep=sep)

def _chain_maker(swp:SQLWithParamsMaker, *vals) -> SQLWithParamsMaker:
    for val in vals:
        swp.append(val)
    return swp

def chain(*vals) -> SQLWithParamsMaker:
    return lambda swp: _chain_maker(swp, *vals)

def clause(keyword:str, *vals) -> SQLWithParamsMaker:
    """ keyword clause """
    return lambda swp: swp.append(sqlword(keyword)).append(*vals)

def opt_clause(keyword:str, val) -> SQLWithParamsMaker:
    """ optional keyword clause """
    if val:
        return lambda swp: swp.append(sqlword(keyword)).append(val)
    return lambda swp: swp

def ordered_column(colexpr) -> SQLWithParamsMaker:
    """ column with DESC or ASC """
    if isinstance(colexpr, UnaryExpr):
        col, odt = colexpr, 'DESC' if colexpr.op == '-' else 'ASC'
    else:
        col, odt = colexpr, 'ASC'
    return lambda swp: swp.append(col).append(odt, raw=True)

def col_as(colexpr, alias:str) -> SQLWithParamsMaker:
    """ column with 'AS' alias """
    if not re.match(r'\w+', alias):
        raise RuntimeError('Invalid alias: "{}"'.format(alias))
    return lambda swp: swp.append(colexpr).append('AS', raw=True).append('"{}"'.format(alias), raw=True) 

def col_opt_as(colexpr, alias:Optional[str]) -> SQLWithParamsMaker:
    """ column """
    if not alias:
        return lambda swp: swp.append(colexpr)
    return col_as(colexpr, alias)

def _sql_op(op:str) -> SQLWithParamsMaker:
    op = op.upper()
    if op in keywords.OP_ALIASES:
        op = keywords.OP_ALIASES[op]
    if op not in keywords.OPS:
        raise RuntimeError('Unknown SQL operator: `{}`'.format(op))
    return lambda swp: swp.append(op, raw=True)


def _sql_func(name:str) -> SQLWithParamsMaker:
    name = name.upper()
    if name in keywords.FUNCS:
        name = keywords.FUNC_ALIASES[name]
    if name not in keywords.FUNCS:
        raise RuntimeError('Unknown SQL function: `{}`'.format(name))
    return lambda swp: swp.append(name, raw=True)

