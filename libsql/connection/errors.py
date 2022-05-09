"""
    Errors
"""

class Error(Exception):
    """ Error base class """

class DatabaseError(Error):
    """ Exception for errors related to the database """

class ProgrammingError(DatabaseError):
    """ Exception for errors programming errors """


class ResponseError(Error):
    """ Response Error """

class NoResultsError(ResponseError):
    """ No Results Error """

class ResultExistsError(ResponseError):
    """ Result Exists Error """


class PreparedStatementError(Error):
    """ Prepared statement error """

class PreparedStatementPrametersError(Error):
    """ Prepared statement parameters error """
