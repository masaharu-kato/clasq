""" SQL Expressions module """
from abc import abstractmethod
import re
from typing import Any, Callable, List, Sequence, Optional
from . import expression
from . import keywords

_IS_DEBUG = True

_HOLDER_STR_ = '%s'

class SQLWithParams:
    """ SQL with parameters (using place-holders) """
    _sql_words: List[str]
    _params: Sequence
    _cached_sql: Optional[str]
    _cached_sql_minimized: Optional[bool]

    def __init__(self, sql=None, params=None):
        self._sql_words = [] if sql is None else sql
        self._params = [] if params is None else params
        self._cached_sql = None
        self._cached_sql_minimized = None

    def append(self, val, *vals, raw:bool=False):
        """ Append new expression(s) """
        
        if raw: # Treat as a raw SQL word
            assert(isinstance(val, str))
            self._append_raw(str(val))

        elif callable(val): 
            val(self) # `val` expects to append some expressions to self
        
        elif isinstance(val, (tuple, list)): # Join elements with ','
            if val:
                self.append(val[0])
                for v in val[1:]:
                    self.append(',', raw=True)
                    self.append(v)

        elif isinstance(val, SQLExprType): # SQLExprType instance has a special method `__sqlout__`
            val.__sqlout__(self) # `__sqlout__` expects to append some expressions to self
        
        else: # Treat as a value
            self._append_raw(_HOLDER_STR_) # Add placeholder string ('%s')
            self._params.append(val) # Add an actual value

        if vals:
            return self.append(*vals, raw=raw) # Process the rest expression(s)

        return self

    def append_clause(self, clause_name:str, *vals, end:str='\n'):
        if not vals:
            return self
        if vals[0] is None or (isinstance(vals[0], (tuple, list)) and not vals[0]):
            # print('vals[0]=', vals[0])
            return self

        self.assert_keyword(clause_name)
        self.append(clause_name, raw=True)
        self.append(*vals)
        if end in (' ', '\n'):
            return self.append(end, raw=True)
        return self

    def _append_raw(self, word:str):
        """ Append a word to the SQL directly """
        self._sql_words.append(word)
        self._cached_sql = None

    @property
    def sql(self, minimize:bool=True) -> str:
        """ Get current SQL text """
        # Returns cache if available
        if self._cached_sql is not None and minimize == self._cached_sql_minimized:
            return self._cached_sql

        self._cached_sql_minimized = minimize

        # Non-minimized mode (Simply join words using a space)
        if not minimize:
            self._cached_sql = ' '.join(self._sql_words)

        # Minimized mode (Use a space only when needed)
        else:
            sql = ''
            pword:str = '' # None 
            for word in self._sql_words:
                if pword and word:
                    if pword[-1] == ',' or ((self._is_word_char(pword[-1]) or pword[-1] == ')') and (self._is_word_char(word[0]) or word[0] == '(')):
                        sql += ' '
                sql += word
                pword = word
            self._cached_sql = sql

        return self._cached_sql


    @property
    def params(self) -> Sequence:
        """ Get current list (sequence) of parameters """
        return self._params


    def execute_on(self, func:Callable[[str, Sequence], Any]):
        """ Execute specified function with current SQL and list of parameters """
        return func(self.sql, self.params)


    @staticmethod
    def assert_keyword(word:str) -> None:
        if not re.match(r'\w+', word):
            raise RuntimeError('Invalid SQL word: `{}`'.format(word))

    @staticmethod
    def _is_word_char(c:str) -> bool:
        return c.isalnum() or c in ('`', '+', '-', '*', '/', '%', '=', '<', '>', '&', '|', '^', '!', '~', '@', "'", '"')


    def __repr__(self):
        return repr((self.sql, self.params))

    def __iter__(self):
        return iter([self.sql, self.params])


SQLWithParamsMaker = Callable[[SQLWithParams], SQLWithParams]



class SQLExprType(expression.ExprType):
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
    def __sqlout__(self, swp:SQLWithParams) -> None:
        """ (Abstract) Output to SQL """
        pass


class UnaryExpr(SQLExprType, expression.UnaryExpr):
    """ UnaryExpr class for SQL """

    def __sqlout__(self, swp:SQLWithParams) -> None:
        swp.append(_sql_op(self.op), self.x)


class BinaryExpr(SQLExprType, expression.BinaryExpr):
    """ UnaryExpr class for SQL """

    def __sqlout__(self, swp:SQLWithParams) -> None:
        swp.append(in_bracket(self.x, _sql_op(self.op), self.y))


class FuncExpr(SQLExprType, expression.FuncExpr):
    """ UnaryExpr class for SQL """

    def __sqlout__(self, swp:SQLWithParams) -> None:
        swp.append(_sql_func(self.name), in_bracket(self.args))


def sqlobj(name) -> SQLWithParamsMaker:
    objname = str(name)
    if not re.match(r'\w+', name):
        raise RuntimeError('Invalid SQL object name: `{}`'.format(name))
    return lambda swp: swp.append('`' + objname + '`', raw=True)


def in_bracket(*vals) -> SQLWithParamsMaker:
    return lambda swp: swp.append('(', raw=True).append(*vals).append(')', raw=True)

# def _joined_maker(swp:SQLWithParamsMaker, *vals, sep:str) -> SQLWithParamsMaker:
#     is_first = True
#     for val in vals:
#         if not is_first:
#             swp.append(sep, raw=True)
#         is_first = False
#         swp.append(val)
#     return swp

# def joined(*vals, sep:str=',') -> SQLWithParamsMaker:
#     return lambda swp: _joined_maker(swp, *vals, sep=sep)

# def opt_joined(*vals, sep:str=',') -> Optional[SQLWithParamsMaker]:
#     if not vals:
#         return None
#     return joined(*vals, sep=sep)

# def _chain_maker(swp:SQLWithParamsMaker, *vals) -> SQLWithParamsMaker:
#     for val in vals:
#         swp.append(val)
#     return swp

# def chain(*vals) -> SQLWithParamsMaker:
#     return lambda swp: _chain_maker(swp, *vals)

# def clause(keyword:str, *vals) -> SQLWithParamsMaker:
#     """ keyword clause """
#     return lambda swp: swp.append(sqlword(keyword)).append(*vals)

# def opt_clause(keyword:str, val) -> SQLWithParamsMaker:
#     """ optional keyword clause """
#     if val:
#         return lambda swp: swp.append(sqlword(keyword)).append(val)
#     return lambda swp: swp

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

