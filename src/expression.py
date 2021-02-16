from copy import copy

class ExprType:
    """ Expression abstract class """

    def __add__(self, y):
        return BinaryExpr('+', self, y)
    def __sub__(self, y):
        return BinaryExpr('-', self, y)
    def __mul__(self, y):
        return BinaryExpr('*', self, y)
    def __truediv__(self, y):
        return BinaryExpr('/', self, y)
    def __floordiv__(self, y):
        return BinaryExpr('//', self, y)
    def __mod__(self, y):
        return BinaryExpr('%', self, y)
    def __divmod__(self, y):
        return BinaryExpr('divmod', self, y)
    def __pow__(self, y):
        return BinaryExpr('**', self, y)
    def __lshift__(self, y):
        return BinaryExpr('<<', self, y)
    def __rshift__(self, y):
        return BinaryExpr('>>', self, y)
    def __and__(self, y):
        return BinaryExpr('&', self, y)
    def __xor__(self, y):
        return BinaryExpr('^', self, y)
    def __or__(self, y):
        return BinaryExpr('|', self, y)
        
    def __radd__(self, y):
        return BinaryExpr('+', y, self)
    def __rsub__(self, y):
        return BinaryExpr('-', y, self)
    def __rmul__(self, y):
        return BinaryExpr('*', y, self)
    def __rtruediv__(self, y):
        return BinaryExpr('/', y, self)
    def __rfloordiv__(self, y):
        return BinaryExpr('//', y, self)
    def __rmod__(self, y):
        return BinaryExpr('%', y, self)
    def __rdivmod__(self, y):
        return BinaryExpr('divmod', y, self)
    def __rpow__(self, y):
        return BinaryExpr('**', y, self)
    def __rlshift__(self, y):
        return BinaryExpr('<<', y, self)
    def __rrshift__(self, y):
        return BinaryExpr('>>', y, self)
    def __rand__(self, y):
        return BinaryExpr('&', y, self)
    def __rxor__(self, y):
        return BinaryExpr('^', y, self)
    def __ror__(self, y):
        return BinaryExpr('|', y, self)

    def __neg__(self):
        return UnaryExpr('-', self)
    def __pos__(self):
        return UnaryExpr('+', self)
    def __abs__(self):
        return UnaryExpr('abs', self)
    def __ceil__(self):
        return UnaryExpr('ceil', self)
    def __floor__(self):
        return UnaryExpr('floor', self)
    def __trunc__(self):
        return UnaryExpr('trunc', self)
    def __invert__(self):
        return UnaryExpr('~', self)
        
    def __int__(self):
        return UnaryExpr('int', self)
    def __float__(self):
        return UnaryExpr('float', self)
    def __round__(self):
        return UnaryExpr('round', self)
    def __complex__(self):
        return UnaryExpr('complex', self)

    # def __bool__(self):
    #     return UnaryExpr('bool', self)
    # def __str__(self):
    #     return UnaryExpr('str', self)
    # def __bytes__(self):
    #     return UnaryExpr('bytes', self)
    # def __len__(self):
    #     return UnaryExpr('len', self)

    def __eq__(self, y):
        return BinaryExpr('==', self, y)
    def __ne__(self, y):
        return BinaryExpr('!=', self, y)
    def __lt__(self, y):
        return BinaryExpr('<', self, y)
    def __le__(self, y):
        return BinaryExpr('<=', self, y)
    def __gt__(self, y):
        return BinaryExpr('>', self, y)
    def __ge__(self, y):
        return BinaryExpr('>=', self, y)


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
