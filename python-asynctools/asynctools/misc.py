#!/usr/bin/env python3
# encoding: utf-8

__all__ = [
    "areversed", "async_iterable", "async_reversible", "async_iter", 
    "async_next", "async_reversed", 
]

from collections.abc import (
    Awaitable, Callable, Iterable, AsyncIterable, AsyncIterator, 
    Reversible, 
)
from typing import cast, overload, Any

from undefined import undefined, Undefined

from .basic import ensure_aiter, ensure_async, to_aiter
from .type import AsyncReversible


def areversed[T](o: AsyncReversible[T], /) -> AsyncIterator[T]:
    cls = type(o)
    if issubclass(cls, AsyncReversible):
        return cls.__areversed__(o)
    raise TypeError(f"{cls!r} object is not async reversible")


def async_iterable(o, /) -> bool:
    try:
        aiter(o)
        return True
    except TypeError:
        return False


def async_reversible(o, /) -> bool:
    return isinstance(o, AsyncReversible)


async def aiter_call[T](
    func: Callable[[], Awaitable[T]] | Callable[[], T], 
    sentinel = undefined, 
    /, 
    threaded: bool = False, 
    catch_exceptions: type[BaseException] | tuple[type[BaseException], ...] = (), 
) -> AsyncIterator[T]:
    func = ensure_async(func, threaded=threaded)
    try:
        if sentinel is undefined:
            while True:
                yield await func()
        else:
            while True:
                r = await func()
                if r is sentinel or r == sentinel:
                    break
                yield r
    except catch_exceptions:
        pass


@overload
def async_iter[T](
    iter: Iterable[T] | AsyncIterable[T], 
    sentinel: Any = undefined, 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    ...
@overload
def async_iter[T](
    iter: Callable[[], Awaitable[T]] | Callable[[], T], 
    sentinel: Any = undefined, 
    /, 
    threaded: bool = False, 
    catch_exceptions: type[BaseException] | tuple[type[BaseException], ...] = (), 
) -> AsyncIterator[T]:
    ...
def async_iter[T](
    iter: Iterable[T] | AsyncIterable[T] | Callable[[], Awaitable[T]] | Callable[[], T], 
    sentinel: Any = undefined, 
    /, 
    threaded: bool = False, 
    catch_exceptions: type[BaseException] | tuple[type[BaseException], ...] = (), 
) -> AsyncIterator[T]:
    if not callable(iter):
        return ensure_aiter(iter, threaded=threaded)
    return aiter_call(iter, sentinel, threaded=threaded, catch_exceptions=catch_exceptions)


@overload
async def async_next[T](
    iter: AsyncIterator[T], 
    /, 
    default: Undefined = undefined, 
) -> T:
    ...
@overload
async def async_next[T, T2](
    iter: AsyncIterator[T], 
    /, 
    default: T2, 
) -> T | T2:
    ...
async def async_next[T, T2](
    iter: AsyncIterator[T], 
    /, 
    default: Undefined | T2 = undefined, 
) -> T | T2:
    if default is undefined:
        return await anext(iter)
    else:
        try:
            return await anext(iter)
        except StopAsyncIteration:
            return cast(T2, default)


def async_reversed[T](
    iter: AsyncReversible[T] | Reversible[T], 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    if isinstance(iter, AsyncReversible):
        return iter.__areversed__()
    return to_aiter(reversed(iter), threaded=threaded)

