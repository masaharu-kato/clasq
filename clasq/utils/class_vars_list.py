"""
    list based on class variables
"""

from typing import Generic, TypeVar

T = TypeVar('T')
class ClassVarsList(Generic[T]):
    """ Listed with class variables """
    
    @classmethod
    def _get(cls, key) -> T:
        return getattr(cls, key)
