"""
    SQL expressions module
"""
from abc import abstractmethod
import re
from typing import Any, Callable, List, Sequence, Optional, Tuple
from . import py_expression
from ..syntax import keywords

_IS_DEBUG = True

_SPACE_ = ' '
_COMMA_ = ','
_BRAC_BEG_ = '('
_BRAC_END_ = ')'
_STR_QUOTE_ = '"'
_HOLDER_STR_ = '%s'


class SQLExprType(py_expression.ExprType):
    """ Expression class for SQL """

    def uop(self, op):
        """ Returns a unary expression object """
        return UnaryExpr(op, self)

    def bop(self, op, y):
        """ Returns a binary expression object """
        return BinaryExpr(op, self, y)

    def rbop(self, op, y):
        """ Returns a reversed binary expression object """
        return BinaryExpr(op, y, self)

    def infunc(self, name, *args):
        """ Returns a function expression object using self as a first parameter """
        return FuncExpr(name, self, *args)

    @abstractmethod
    def sql_with_params(self) -> Tuple[str, List]:
        """ Output sql and its placeholder parameters """


class UnaryExpr(SQLExprType, py_expression.UnaryExpr):
    """ UnaryExpr class for SQL """

    def sql_with_params(self):
        return sql_with_params(SQLOpSymbol(self.op), self.x, bracket=True)


class BinaryExpr(SQLExprType, py_expression.BinaryExpr):
    """ UnaryExpr class for SQL """

    def sql_with_params(self):
        return sql_with_params(self.x, SQLOpSymbol(self.op), self.y, bracket=True)


class FuncExpr(SQLExprType, py_expression.FuncExpr):
    """ UnaryExpr class for SQL """

    def sql_with_params(self):
        return sql_with_params(SQLFuncName(self.name), sql_with_params(*self.args, bracket=True))


class RawExpr(SQLExprType):

    def __init__(self, expr):
        self.expr = expr

    def sql_with_params(self) -> Tuple[str, List]:
        return self.expr, []


def sql_with_params(*vals, comma:bool=False, bracket:bool=False, end:str='') -> Tuple[str, list]:
    """ Process SQL with parameters """
    sql = ''
    params = []
    
    if bracket:
        sql += _BRAC_BEG_

    for i, val in enumerate(vals):
        csql, cparams = None, None

        if isinstance(val, (tuple, list)): # Join elements with ','
            if val:
                csql, cparams = sql_with_params(*val, comma=True)

        elif isinstance(val, SQLExprType): # SQLExprType instance has a special method `sql_with_params`
            csql, cparams = val.sql_with_params() # `sql_with_params` expects to append some expressions to self
            
        else: # Treat as a parameter value
            csql, cparams = _HOLDER_STR_, val # Placeholder string ('%s') and an actual value
        
        if csql:
            if comma and i > 0:
                sql += _COMMA_
            sql += _SPACE_ + csql
            if cparams:
                params.extend(cparams)
    
    if bracket:
        sql += _BRAC_END_

    if end:
        sql += end

    return sql, params


def SQLOpSymbol(op:str) -> RawExpr:
    """ Check the SQL operator symbol and return it as a raw expression """
    op = op.upper()
    if op in keywords.OP_ALIASES:
        op = keywords.OP_ALIASES[op]
    if op not in keywords.OPS:
        raise RuntimeError('Unknown SQL operator: `{}`'.format(op))
    return RawExpr(op)


def SQLFuncName(name:str) -> RawExpr:
    """ Check the SQL function name and return it as a raw expression """
    name = name.upper()
    if name in keywords.FUNCS:
        name = keywords.FUNC_ALIASES[name]
    if name not in keywords.FUNCS:
        raise RuntimeError('Unknown SQL function: `{}`'.format(name))
    return RawExpr(name)


def SQLKeyword(keyword:str) -> RawExpr:
    """ Check the keyword and return it as a raw expression """
    if not re.match(r'\w+', keyword):
        raise RuntimeError('Invalid SQL word: `{}`'.format(keyword))
    return RawExpr(keyword)


def QuotedName(keyword:str) -> RawExpr:
    assert not '"' in keyword
    return RawExpr('"%s"' % keyword)
