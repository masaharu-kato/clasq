"""
    Syntax Errors
"""

class SyntaxError(RuntimeError):
    """ Syntax Error """

class ObjectError(RuntimeError):
    """ Object Error """

class ObjectArgsError(ObjectError):
    """ Object arguments Error """

class ObjectArgNumError(ObjectArgsError):
    """ Object arguments number Error """

class ObjectArgTypeError(ObjectArgsError, TypeError):
    """ Object arguments type Error """

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
