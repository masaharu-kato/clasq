"""
    Name conversion functions
"""
from __future__ import annotations
import re

_CAMEL_TO_SNAKE_RE_1 = re.compile(r'(.)([A-Z][a-z]+)')
_CAMEL_TO_SNAKE_RE_2 = re.compile(r'([a-z0-9])([A-Z])')

def camel_to_snake(name: str) -> str:
    name = _CAMEL_TO_SNAKE_RE_1.sub(r'\1_\2', name)
    return _CAMEL_TO_SNAKE_RE_2.sub(r'\1_\2', name).lower()


def snake_to_camel(name: str) -> str:
    return ''.join(word.title() for word in name.split('_'))
