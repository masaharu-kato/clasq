"""
    Syntax Errors
"""

class SyntaxError(Exception):
    """ Syntax Error """

class ObjectError(Exception):
    """ Object Error """

class ObjectArgsError(ObjectError):
    """ Object arguments Error """

class ObjectArgNumError(ObjectArgsError):
    """ Object arguments number Error """

class ObjectArgTypeError(ObjectArgsError, TypeError):
    """ Object arguments type Error """

class ObjectArgValueError(ObjectArgsError, ValueError):
    """ Object arguments value Error """

class ObjectNotSpecifiedError(ObjectArgsError):
    """ Object arguments not specified Error """

class ObjectNotFoundError(ObjectError, KeyError):
    """ Object not found Error """

class NotaSelfObjectError(ObjectError):
    """ Not a self object Error """

class ObjectNotSetError(ObjectError):
    """ Object not set Error """

class ObjectAlreadySetError(ObjectError):
    """ Object already set Error """

class ObjectNameAlreadyExistsError(ObjectError):
    """ Object name already exists Error """

class ObjectAmbiguousError(ObjectError):
    """ Object is ambiguous Error """

class ObjectExprError(ObjectError):
    """ Object expression error """


class QueryError(Exception):
    """ Query Error  """

class QueryValueError(QueryError, ValueError):
    """ Query Value Error """

class QueryTypeError(QueryError, TypeError):
    """ Query Type Error """

class QueryArgumentError(QueryError):
    """ Query Argument Error """

