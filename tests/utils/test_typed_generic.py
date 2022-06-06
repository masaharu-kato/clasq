"""
    Test Typed Generic
"""
from typing import Literal, TypeVar, Generic

import pytest

from clasq.utils.typed_generic import OptionalTypedGeneric, OptionalTypedGenericABC, TypedGeneric, GenericArgumentsNotSpecifiedError, TypedGenericABC

@pytest.mark.parametrize(
    'base_generic_type',
    [TypedGeneric, TypedGenericABC, OptionalTypedGeneric, OptionalTypedGenericABC]
)
def test_typed_generic_single_arg(base_generic_type: type[TypedGenericABC]):
    T = TypeVar('T')
    class MyClass(base_generic_type, Generic[T]):  # type: ignore
        """ My class """
    
    cls = MyClass
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._typing_generic
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._generic_origin
    assert cls._generic_argkeys == ('T', )

    cls = MyClass[int]
    # assert cls._typing_generic.__name__ == 'MyClass[int]'
    assert cls._generic_origin == MyClass
    assert cls._generic_argkeys == ('T', )
    assert cls._generic_argvals == (int, )
    assert T@cls is int  # type: ignore

    cls2 = MyClass[int]
    assert cls is cls2

    cls3 = MyClass[float]
    assert cls2 is not cls3
    assert cls3._generic_argvals == (float, )

    inst = MyClass[int]()
    assert T@inst is int


@pytest.mark.parametrize(
    'base_generic_type',
    [TypedGeneric, TypedGenericABC, OptionalTypedGeneric, OptionalTypedGenericABC]
)
def test_typed_generic_multi_args(base_generic_type: type[TypedGenericABC]):
    T1 = TypeVar('T1')
    T2 = TypeVar('T2')
    class MyClass(base_generic_type, Generic[T1, T2]):  # type: ignore
        """ My class """
    
    cls = MyClass
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._typing_generic
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._generic_origin
    assert cls._generic_argkeys == ('T1', 'T2')

    cls = MyClass[int, str]
    # assert cls._typing_generic.__name__ == 'MyClass[int, str]'
    assert cls._generic_origin == MyClass
    assert cls._generic_argkeys == ('T1', 'T2')
    assert cls._generic_argvals == (int, str)
    assert T1@cls is int  # type: ignore
    assert T2@cls is str  # type: ignore

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


@pytest.mark.parametrize(
    'base_generic_type',
    [TypedGeneric, TypedGenericABC]
)
def test_nonopt_typed_generic_single_arg(base_generic_type: type[TypedGenericABC]):
    T = TypeVar('T')
    class MyClass(base_generic_type, Generic[T]):  # type: ignore
        """ My class """
    
    cls = MyClass
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._typing_generic
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._generic_origin
    assert cls._generic_argkeys == ('T', )
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._generic_argvals


@pytest.mark.parametrize(
    'base_generic_type',
    [TypedGeneric, TypedGenericABC]
)
def test_nonopt_typed_generic_multi_args(base_generic_type: type[TypedGenericABC]):
    T1 = TypeVar('T1')
    T2 = TypeVar('T2')
    class MyClass(base_generic_type, Generic[T1, T2]):  # type: ignore
        """ My class """
    
    cls = MyClass
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._typing_generic
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._generic_origin
    assert cls._generic_argkeys == ('T1', 'T2')
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._generic_argvals


@pytest.mark.parametrize(
    'base_generic_type',
    [OptionalTypedGeneric, OptionalTypedGenericABC]
)
def test_optional_typed_generic_single_arg(base_generic_type: type[OptionalTypedGenericABC]):
    T = TypeVar('T')
    class MyClass(base_generic_type, Generic[T]):  # type: ignore
        """ My class """
    
    cls = MyClass
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._typing_generic
    with pytest.raises(GenericArgumentsNotSpecifiedError):
        cls._generic_origin
    assert cls._generic_argkeys == ('T', )
    assert cls._generic_argvals == ()

    cls = MyClass[int]
    # assert cls._typing_generic.__name__ == 'MyClass[int]'
    assert cls._generic_origin == MyClass
    assert cls._generic_argkeys == ('T', )
    assert cls._generic_argvals == (int, )
    assert T@cls is int  # type: ignore


@pytest.mark.parametrize(
    'base_generic_type',
    [TypedGeneric, TypedGenericABC, OptionalTypedGeneric, OptionalTypedGenericABC]
)
def test_typed_generic_multiple_classes(base_generic_type: type[TypedGenericABC]):
    T = TypeVar('T')
    class MyClassBase(base_generic_type, Generic[T]):  # type: ignore
        """ My class """
    
    class MySpecialFoo(MyClassBase, Generic[T]):
        """ My special foo class """
    
    L = TypeVar('L', bound=int)
    class MySpecialBar(MyClassBase, Generic[L]):
        """ My special bar class """
    
    class MySpecialBaz(MyClassBase, Generic[T, L]):
        """ My special baz class """

    assert MySpecialFoo._generic_argkeys == ('T', )
    assert MySpecialBar._generic_argkeys == ('L', )
    assert MySpecialBaz._generic_argkeys == ('T', 'L')

    cls1 = MyClassBase[int]
    cls2 = MySpecialBaz[float, Literal[10]]
    cls3 = MyClassBase[str]
    cls4 = MySpecialBar[Literal[64]]
    cls5 = MySpecialFoo[bytes]
    cls6 = MySpecialBar[Literal[40]]

    assert cls1._generic_origin is MyClassBase
    assert cls2._generic_origin is MySpecialBaz
    assert cls3._generic_origin is MyClassBase
    assert cls4._generic_origin is MySpecialBar
    assert cls5._generic_origin is MySpecialFoo
    assert cls6._generic_origin is MySpecialBar

    assert cls1._generic_argvals == (int,)
    assert cls2._generic_argvals == (float, Literal[10])
    assert cls3._generic_argvals == (str,)
    assert cls4._generic_argvals == (Literal[64],)
    assert cls5._generic_argvals == (bytes,)
    assert cls6._generic_argvals == (Literal[40],)

    assert T@cls3 is str
    assert T@cls2 is float
    assert L@cls2 == 10
    assert L@cls4 == 64
    assert T@cls5 is bytes
    assert T@cls1 is int
    assert L@cls6 == 40

    inst1 = MyClassBase[int]()
    inst2 = MySpecialBaz[float, Literal[10]]()
    inst3 = MyClassBase[str]()
    inst4 = MySpecialBar[Literal[64]]()
    inst5 = MySpecialFoo[bytes]()
    inst6 = MySpecialBar[Literal[40]]()

    assert T@inst3 is str
    assert T@inst2 is float
    assert L@inst2 == 10
    assert L@inst4 == 64
    assert T@inst5 is bytes
    assert T@inst1 is int
    assert L@inst6 == 40


# def main():
#     test_typed_generic_multi_args()

# if __name__ == '__main__':
#     main()

