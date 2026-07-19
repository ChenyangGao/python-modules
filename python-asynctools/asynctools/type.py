#!/usr/bin/env python3
# encoding: utf-8

__all__ = ["AsyncReversible"]

from abc import abstractmethod
from collections.abc import AsyncIterable, AsyncIterator
from typing import runtime_checkable, Protocol

try:
    from collections.abc import _check_methods # type: ignore
except ImportError:
    def _check_methods(C, /, *methods):
        mro = C.__mro__
        for method in methods:
            for B in mro:
                if method in B.__dict__:
                    if B.__dict__[method] is None:
                        return NotImplemented
                    break
            else:
                return NotImplemented
        return True


@runtime_checkable
class SupportsBool(Protocol):
    def __bool__(self, /) -> bool:
        ...


@runtime_checkable
class SupportsAdd(Protocol):
    def __add__(self, other, /):
        ...


@runtime_checkable
class SupportsRAdd[V](Protocol):
    def __radd__(self, other, /):
        ...


class AsyncReversible[T](AsyncIterable[T]):
    __slots__ = ()

    @abstractmethod
    async def __areversed__(self, /) -> AsyncIterator[T]:
        while False:
            yield

    @classmethod
    def __subclasshook__(cls, C):
        if cls is AsyncReversible:
            return _check_methods(C, "__areversed__", "__aiter__")
        return NotImplemented

