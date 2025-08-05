#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io/>"
__version__ = (0, 0, 1)
__all__ = [
    "ensure_async", "ensure_awaitable", "ensure_coroutine", 
    "ensure_cm", "ensure_acm", "ensure_enum", "ensure_str", 
    "ensure_bytes", "ensure_buffer", "ensure_functype", 
]

from collections.abc import Buffer
from collections import UserString
from contextlib import (
    asynccontextmanager, contextmanager, 
    AbstractAsyncContextManager, AbstractContextManager, 
)
from enum import Enum
from functools import update_wrapper
from inspect import isawaitable, iscoroutine, iscoroutinefunction
from types import FunctionType

from integer_tool import int_to_bytes


async def _as_async(o, /, unpack: bool = False):
    if unpack and isawaitable(o):
        return await o
    return o


@contextmanager
def _as_cm(o=None, /, unpack: bool = False):
    if unpack:
        if isinstance(o, AbstractContextManager):
            with o as v:
                yield v
            return
    yield o


@asynccontextmanager
async def _as_acm(o=None, /, unpack: bool = False):
    if unpack:
        if isinstance(o, AbstractContextManager):
            with o as v:
                yield v
            return
        elif isinstance(o, AbstractAsyncContextManager):
            async with o as v:
                yield v
            return
    yield o


def ensure_async(o, /):
    if iscoroutinefunction(o):
        return o
    return _as_async(o, unpack=True)


def ensure_awaitable(o, /):
    if isawaitable(o):
        return o
    return _as_async(o)


def ensure_coroutine(o, /):
    if iscoroutine(o):
        return o
    return _as_async(o)


def ensure_cm(o, /) -> AbstractContextManager:
    if isinstance(o, AbstractContextManager):
        return o
    return _as_cm(o)


def ensure_acm(o, /) -> AbstractAsyncContextManager:
    if isinstance(o, AbstractAsyncContextManager):
        return o
    return _as_acm(o)


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

