"""
    Syntax Errors
"""

class SyntaxError(RuntimeError):
    """ Syntax Error """

class ObjectError(RuntimeError):
    """ Object Error """

class ObjectArgumentsError(ObjectError):
    """ Object arguments Error """

class ObjectArgumentsNumberError(ObjectArgumentsError):
    """ Object arguments number Error """

class ObjectArgumentsTypeError(ObjectArgumentsError, TypeError):
    """ Object arguments type Error """

class ObjectNotSpecifiedError(ObjectArgumentsError):
    """ Object arguments not specified Error """

class ObjectNotFoundError(ObjectError, KeyError):
    """ Object not found Error """

class NotaSelfObjectError(ObjectError):
    """ Not a self object Error """

class ObjectNotSetError(ObjectError):
    """ Object not set Error """

class ObjectAlreadySetError(ObjectError):
    """ Object already set Error """
