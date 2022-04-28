"""
    Errors
"""

class Error(Exception):
    """ Error base class """

class DatabaseError(Error):
    """ Exception for errors related to the database """

class ProgrammingError(DatabaseError):
    """ Exception for errors programming errors """
