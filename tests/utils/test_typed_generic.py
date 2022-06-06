"""
    Test Typed Generic
"""
from typing import TypeVar, Generic

import pytest

from clasq.utils.typed_generic import TypedGeneric, GenericArgumentsNotSpecifiedError


def test_typed_generic_single_arg():
    T = TypeVar('T')
    class MyClass(TypedGeneric, Generic[T]):
        """ My class """
    
    cls = MyClass
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._typing_generic
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._generic_origin
    assert cls._generic_argkeys == ('T', )
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._generic_argvals

    cls = MyClass[int]
    # assert cls._typing_generic.__name__ == 'MyClass[int]'
    assert cls._generic_origin == MyClass
    assert cls._generic_argkeys == ('T', )
    assert cls._generic_argvals == (int, )
    assert T@cls is int

    cls2 = MyClass[int]
    assert cls is cls2

    cls3 = MyClass[float]
    assert cls2 is not cls3
    assert cls3._generic_argvals == (float, )

    inst = MyClass[int]()
    assert T@inst is int


def test_typed_generic_multi_args():
    T1 = TypeVar('T1')
    T2 = TypeVar('T2')
    class MyClass(TypedGeneric, Generic[T1, T2]):
        """ My class """
    
    cls = MyClass
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._typing_generic
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._generic_origin
    assert cls._generic_argkeys == ('T1', 'T2')
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._generic_argvals

    cls = MyClass[int, str]
    # assert cls._typing_generic.__name__ == 'MyClass[int, str]'
    assert cls._generic_origin == MyClass
    assert cls._generic_argkeys == ('T1', 'T2')
    assert cls._generic_argvals == (int, str)
    assert T1@cls is int
    assert T2@cls is str

    cls2 = MyClass[int, str]
    assert cls is cls2

    cls3 = MyClass[float, str]
    assert cls2 is not cls3
    assert cls3._generic_argvals == (float, str)
    
    cls4 = MyClass[str, float]
    assert cls3 is not cls4
    assert cls4._generic_argvals == (str, float)

    inst = MyClass[int, str]()
    assert T1@inst is int
    assert T2@inst is str



# def main():
#     test_typed_generic_multi_args()

# if __name__ == '__main__':
#     main()

