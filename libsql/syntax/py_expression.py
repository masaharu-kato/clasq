""" 
    SQL expressions module
"""

from abc import abstractmethod

class ExprABC:
    """ Expression Abstract class """

    def op(self, *args): # 1st: op, 2nd: y (optional)
        """ Get unary or binary expression """
        if len(args) > 2:
            raise RuntimeError('Too many arguments.')
        if len(args) == 2:
            return self.uop(args[0])
        return self.bop(args[0], args[1])

    @abstractmethod
    def uop(self, op):
        """ Unary operation """

    @abstractmethod
    def bop(self, op, y):
        """ Binary operation """

    @abstractmethod
    def rbop(self, op, y):
        """ Reversed binary operation """

    @abstractmethod
    def infunc(self, name, *args):
        """ In-function operation """


class ExprType(ExprABC):
    """ Expression abstract class """

    def uop(self, op):
        """ Get unary expression """ 
        return UnaryExpr(op, self)

    def bop(self, op, y):
        """ Get binary expression """ 
        return BinaryExpr(op, self, y)

    def rbop(self, op, y):
        """ Get reversed binary expression """ 
        return BinaryExpr(op, y, self)

    def infunc(self, name, *args):
        """ Get in-function expression """ 
        return FuncExpr(name, self, *args)


    def __add__(self, y):
        return self.bop('+', y)
    def __sub__(self, y):
        return self.bop('-', y)
    def __mul__(self, y):
        return self.bop('*', y)
    def __truediv__(self, y):
        return self.bop('/', y)
    def __floordiv__(self, y):
        return self.bop('//', y)
    def __mod__(self, y):
        return self.bop('%', y)
    def __divmod__(self, y):
        return self.bop('divmod', y)
    def __pow__(self, y):
        return self.bop('**', y)
    def __lshift__(self, y):
        return self.bop('<<', y)
    def __rshift__(self, y):
        return self.bop('>>', y)
    def __and__(self, y):
        return self.bop('&', y)
    def __xor__(self, y):
        return self.bop('^', y)
    def __or__(self, y):
        return self.bop('|', y)

    def __eq__(self, y):
        return self.bop('==', y)
    def __ne__(self, y):
        return self.bop('!=', y)
    def __lt__(self, y):
        return self.bop('<', y)
    def __le__(self, y):
        return self.bop('<=', y)
    def __gt__(self, y):
        return self.bop('>', y)
    def __ge__(self, y):
        return self.bop('>=', y)
        
    def __radd__(self, y):
        return self.rbop('+', y)
    def __rsub__(self, y):
        return self.rbop('-', y)
    def __rmul__(self, y):
        return self.rbop('*', y)
    def __rtruediv__(self, y):
        return self.rbop('/', y)
    def __rfloordiv__(self, y):
        return self.rbop('//', y)
    def __rmod__(self, y):
        return self.rbop('%', y)
    def __rdivmod__(self, y):
        return self.rbop('divmod', y)
    def __rpow__(self, y):
        return self.rbop('**', y)
    def __rlshift__(self, y):
        return self.rbop('<<', y)
    def __rrshift__(self, y):
        return self.rbop('>>', y)
    def __rand__(self, y):
        return self.rbop('&', y)
    def __rxor__(self, y):
        return self.rbop('^', y)
    def __ror__(self, y):
        return self.rbop('|', y)

    def __neg__(self):
        return self.uop('-')
    def __pos__(self):
        return self.uop('+')
    def __invert__(self):
        return self.uop('~')
        
    def __abs__(self):
        return self.infunc('abs')
    def __ceil__(self):
        return self.infunc('ceil')
    def __floor__(self):
        return self.infunc('floor')
    def __trunc__(self):
        return self.infunc('trunc')

    def __int__(self):
        return self.infunc('int')
    def __float__(self):
        return self.infunc('float')
    def __round__(self):
        return self.infunc('round')
    def __complex__(self):
        return self.infunc('complex')

    # def __bool__(self):
    #     return self.self('bool')
    # def __str__(self):
    #     return self.self('str')
    # def __bytes__(self):
    #     return self.self('bytes')
    # def __len__(self):
    #     return self.self('len')


class Expr(ExprType):
    """ Expression objerct with any value """
    def __init__(self, v):
        self.v = _get_v(v)

    def __repr__(self):
        return repr(self.v)


class UnaryExpr(ExprType):
    """ Unary expression class """
    def __init__(self, op, x):
        self.op = op
        self.x = _get_v(x)

    def __repr__(self):
        return f'({self.op} {self.x})'


class BinaryExpr(UnaryExpr): 
    """ General expression class """
    def __init__(self, op, x, y):
        super().__init__(op=op, x=x)
        self.y = _get_v(y)

    def __repr__(self):
        return f'({self.x} {self.op} {self.y})'

    # def __iadd__(self, y):
    #     self.x, self.op, self.y = copy(self), '+', y
    # def __isub__(self, y):
    #     self.x, self.op, self.y = copy(self), '-', y
    # def __imul__(self, y):
    #     self.x, self.op, self.y = copy(self), '*', y
    # def __imod__(self, y):
    #     self.x, self.op, self.y = copy(self), '%', y
    # def __itruediv__(self, y):
    #     self.x, self.op, self.y = copy(self), '/', y
    # def __ifloordiv__(self, y):
    #     self.x, self.op, self.y = copy(self), '//', y
    # def __ipow__(self, y):
    #     self.x, self.op, self.y = copy(self), '**', y
    # def __ilshift__(self, y):
    #     self.x, self.op, self.y = copy(self), '<<', y
    # def __irshift__(self, y):
    #     self.x, self.op, self.y = copy(self), '>>', y
    # def __iand__(self, y):
    #     self.x, self.op, self.y = copy(self), '&', y
    # def __ixor__(self, y):
    #     self.x, self.op, self.y = copy(self), '^', y
    # def __ior__(self, y):
    #     self.x, self.op, self.y = copy(self), '|', y


class FuncExpr(ExprType):
    """ Function expression class """
    def __init__(self, name, *args):
        self.name = name
        self.args = [_get_v(v) for v in args]

    def __repr__(self):
        return self.name+'('+', '.join(str(arg) for arg in self.args)+')'


class Func:
    """ Function class for function expression """
    def __init__(self, name):
        self.name = name

    def __call__(self, *args):
        return FuncExpr(self.name, *args) 


def _get_v(v):
    if isinstance(v, Expr):
        return v.v
    return v
