"""
    SQL expressions module
"""
from abc import abstractmethod
import re
from typing import List, Optional, Tuple
from .expr_class import FuncABC
from .expr_type import ColumnExpr, Expr, ExprType
from .operators import OP

_SPACE_ = ' '
_COMMA_ = ','
_BRAC_BEG_ = '('
_BRAC_END_ = ')'
_STR_QUOTE_ = '"'
_HOLDER_STR_ = '%s'


# class RawExpr(SQLExprType):

#     def __init__(self, expr):
#         self.expr = expr

#     def sql_with_params(self) -> Tuple[str, List]:
#         return self.expr, []


def make_expr(*vals, **kwargs) -> ExprType:
    """ """
    return make_expr_one(list(vals) if not kwargs else [*vals, kwargs], join_ops=[OP.AND, OP.OR], dict_op=OP.EQ)
    

def make_expr_one(val, *, join_ops: Optional[List[FuncABC]]=None, dict_op: Optional[FuncABC] = None) -> ExprType:
    """ """

    if isinstance(val, ExprType):
        return val

    if join_ops:
        assert len(join_ops) >= 1
        c_join_op = join_ops[0]
        next_make_expr = lambda val: make_expr_one(val, join_ops=[*join_ops[1:], c_join_op], dict_op=dict_op)
    else:
        c_join_op = None
        next_make_expr = lambda val: make_expr_one(val, dict_op=dict_op)

    if c_join_op and isinstance(val, list):
        if len(val) == 1:
            return next_make_expr(val[0])
        return c_join_op.call(*(next_make_expr(v) for v in val))

    if isinstance(val, tuple) and len(val) >= 1:
        if isinstance(val[0], FuncABC):
            if len(val) == 1:
                return val[0].call()
            if len(val) == 2:
                if isinstance(val[1], list):
                    return val[0].call(*val[1])
                return val[0].call(next_make_expr(val[1]))
            
        if len(val) == 3 and isinstance(val[1], FuncABC):
            return val[1].call(next_make_expr(val[0]), next_make_expr(val[2]))

        raise RuntimeError('Invalid form of tuple.')

    if dict_op and isinstance(val, dict):
        return OP.AND.call(*(dict_op.call(ColumnExpr(c.encode()), v) for c, v in val.items()))

    return Expr(val)



def expr_to_sql(*vals, comma:bool=False, bracket:bool=False, end:str='') -> Tuple[str, list]:
    """ Generate a pair of (SQL statement, its parameters) from the any type values """
    sql = ''
    params = []
    
    if bracket:
        sql += _BRAC_BEG_

    for i, val in enumerate(vals):
        csql, cparams = None, None

        if isinstance(val, (tuple, list)): # Join elements with ','
            if val:
                csql, cparams = expr_to_sql(*val, comma=True)

        elif isinstance(val, ExprType): # SQLExprType instance has a special method `sql_with_params`
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


class FuncExpr(ExprType): 
    """ General expression class """
    def __init__(self, func: FuncABC, *args):
        self._func = func
        self._args: List[ExprType] = [make_expr_one(arg) for arg in args]

    @property
    def func(self):
        return self._func

    @property
    def args(self):
        return self._args

    def get_statement_data(self) -> Tuple[bytes, list]:
        return self._func.get_statement_data_with_args(self._args)

    def __repr__(self):
        return self._func.repr_with_args(self._args)


FuncABC._FuncExpr = FuncExpr



# def SQLOpSymbol(op:str) -> RawExpr:
#     """ Check the SQL operator symbol and return it as a raw expression """
#     op = op.upper()
#     if op in keywords.OP_ALIASES:
#         op = keywords.OP_ALIASES[op]
#     if op not in keywords.OPS:
#         raise RuntimeError('Unknown SQL operator: `{}`'.format(op))
#     return RawExpr(op)


# def SQLFuncName(name:str) -> RawExpr:
#     """ Check the SQL function name and return it as a raw expression """
#     name = name.upper()
#     if name in keywords.FUNCS:
#         name = keywords.FUNC_ALIASES[name]
#     if name not in keywords.FUNCS:
#         raise RuntimeError('Unknown SQL function: `{}`'.format(name))
#     return RawExpr(name)


# def SQLKeyword(keyword:str) -> RawExpr:
#     """ Check the keyword and return it as a raw expression """
#     if not re.match(r'\w+', keyword):
#         raise RuntimeError('Invalid SQL word: `{}`'.format(keyword))
#     return RawExpr(keyword)


# def QuotedName(keyword:str) -> RawExpr:
#     assert not '"' in keyword
#     return RawExpr('"%s"' % keyword)
