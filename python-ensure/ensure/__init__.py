#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io/>"
__version__ = (0, 0, 2)
__all__ = [
    "ensure_async", "ensure_awaitable", "ensure_coroutine", 
    "ensure_iter", "ensure_aiter", "ensure_cm", "ensure_acm", 
    "ensure_enum", "ensure_str", "ensure_bytes", "ensure_buffer", 
    "ensure_functype", 
]

from collections import UserString
from collections.abc import (
    AsyncGenerator, AsyncIterable, AsyncIterator, Awaitable, Buffer, 
    Callable, Coroutine, Generator, Iterable, Iterator, 
)
from contextlib import (
    asynccontextmanager, contextmanager, 
    AbstractAsyncContextManager, AbstractContextManager, 
)
from enum import Enum
from functools import update_wrapper
from inspect import isawaitable, iscoroutine, iscoroutinefunction
from types import FunctionType
from typing import Any, AsyncContextManager, ContextManager

from integer_tool import int_to_bytes


def _to_async[**Args, T](
    func: Callable[Args, Awaitable[T]] | Callable[Args, T], 
    /, 
) -> Callable[Args, Coroutine[None, None, T]]:
    async def wrapper(*args: Args.args, **kwds: Args.kwargs) -> T:
        ret = func(*args, **kwds)
        if isawaitable(ret):
            ret = await ret
        return ret
    return update_wrapper(wrapper, func)


def ensure_async[**Args, T](
    func: Callable[Args, Awaitable[T]] | Callable[Args, T], 
    /, 
) -> Callable[Args, Coroutine[Any, Any, T]]:
    if iscoroutinefunction(func):
        return func
    return _to_async(func)


async def _to_coro[T](o: Awaitable[T] | T, /) -> T:
    if isawaitable(o):
        return await o
    return o


def ensure_awaitable[T](o: Awaitable[T] | T, /) -> Awaitable[T]:
    if isawaitable(o):
        return o
    return _to_coro(o)


def ensure_coroutine[T](o: Awaitable[T] | T, /) -> Coroutine[Any, Any, T]:
    if iscoroutine(o):
        return o
    return _to_coro(o)


def _to_iter[T](o: T, /) -> Iterator[T]:
    yield o


def ensure_iter[T](it: Iterable[T] | T, /) -> Iterator[T]:
    if isinstance(it, Iterable):
        return iter(it)
    return _to_iter(it)


async def _to_aiter[T](o: Iterable[T] | T, /) -> AsyncIterator[T]:
    if isinstance(o, Iterable):
        for e in o:
            yield e
    else:
        yield o


def ensure_aiter[T](it: AsyncIterable[T] | Iterable[T] | T, /) -> AsyncIterator[T]:
    if isinstance(it, AsyncIterable):
        return aiter(it)
    return _to_aiter(it)


@contextmanager
def _to_cm[T](o: T, /) -> Generator[T]:
    yield o


def ensure_cm[T](o: ContextManager[T] | T, /) -> ContextManager[T]:
    if isinstance(o, AbstractContextManager):
        return o
    return _to_cm(o)


@asynccontextmanager
async def _to_acm[T](o: ContextManager[T] | T, /) -> AsyncGenerator[T]:
    if isinstance(o, AbstractContextManager):
        with o as v:
            yield v
    else:
        yield o


def ensure_acm[T](o: AsyncContextManager[T] | ContextManager[T] | T, /) -> AbstractAsyncContextManager:
    if isinstance(o, AbstractAsyncContextManager):
        return o
    return _to_acm(o)


def ensure_enum[T: Enum](cls: type[T], val, /) -> T:
    if isinstance(val, cls):
        return val
    elif isinstance(val, str):
        try:
            return cls[val]
        except KeyError:
            pass
    return cls(val)


def ensure_str(o, /, encoding: str = "utf-8", errors: str = "strict") -> str:
    if isinstance(o, str):
        return o
    elif isinstance(o, Buffer):
        return str(o, encoding, errors)
    return str(o)


def ensure_bytes(o, /, encoding: str = "utf-8", errors: str = "strict") -> bytes:
    if isinstance(o, bytes):
        return o
    elif isinstance(o, memoryview):
        return o.tobytes()
    elif isinstance(o, Buffer):
        return bytes(o)
    elif isinstance(o, int):
        return int_to_bytes(o)
    elif isinstance(o, (str, UserString)):
        return o.encode(encoding, errors)
    try:
        return bytes(o)
    except Exception:
        return bytes(str(o), encoding, errors)


def ensure_buffer(o, /, encoding: str = "utf-8", errors: str = "strict") -> Buffer:
    if isinstance(o, Buffer):
        return o
    elif isinstance(o, int):
        return int_to_bytes(o)
    elif isinstance(o, (str, UserString)):
        return o.encode(encoding, errors)
    try:
        return bytes(o)
    except Exception:
        return bytes(str(o), encoding, errors)


def ensure_functype(f, /):
    if isinstance(f, FunctionType):
        return f
    elif callable(f):
        return update_wrapper(lambda *args, **kwds: f(*args, **kwds), f)
    else:
        return lambda: f

