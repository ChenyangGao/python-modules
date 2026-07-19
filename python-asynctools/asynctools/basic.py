#!/usr/bin/env python3
# encoding: utf-8

__all__ = [
    "to_coroutine", "ensure_awaitable", "ensure_coroutine", "to_async", 
    "ensure_async", "to_aiter", "ensure_aiter", "to_async_cm", "ensure_async_cm", 
    "coroutine", "coroutinefunction", "as_thread", 
]

from asyncio import sleep, to_thread
from collections.abc import (
    Awaitable, AsyncIterable, AsyncIterator, Callable, Coroutine, 
    Generator, Iterable, 
)
from contextlib import asynccontextmanager, AbstractAsyncContextManager, AbstractContextManager
from inspect import isawaitable, iscoroutine, iscoroutinefunction
from typing import cast, Any, AsyncContextManager

from decotools import decorated


async def to_coroutine[T](o: T, /) -> T:
    return o


def ensure_awaitable(o, /) -> Awaitable:
    if isawaitable(o):
        return o
    return to_coroutine(o)


async def _to_coro[T](o: Awaitable[T] | T, /) -> T:
    if isawaitable(o):
        return await o
    return o


def ensure_coroutine(o, /) -> Coroutine:
    if iscoroutine(o):
        return o
    return _to_coro(o)


def to_async[**Args, T](
    o: Callable[Args, Awaitable[T]] | Callable[Args, T] | T, 
    /, 
    threaded: bool = False, 
) -> Callable[Args, Coroutine[Any, Any, T]]:
    if callable(o):
        if threaded:
            return lambda *a, **k: to_thread(o, *a, **k)
        return lambda *a, **k: ensure_coroutine(o(*a, **k))
    return lambda *a, **k: to_coroutine(o)


def ensure_async[**Args, T](
    func: Callable[Args, Awaitable[T]] | Callable[Args, T], 
    /, 
    threaded: bool = False, 
) -> Callable[Args, Coroutine[Any, Any, T]]:
    if iscoroutinefunction(func):
        return func
    return to_async(func, threaded=threaded)


async def to_aiter[T](
    it: Iterable[T] | T, 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    if isinstance(it, Iterable):
        if threaded:
            get_next = iter(it).__next__
            try:
                while True:
                    yield await to_thread(get_next)
            except StopIteration:
                pass
        else:
            for e in it:
                yield e
    else:
        yield it


def ensure_aiter[T, R](
    it: AsyncIterable[T] | Iterable[T], 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    try:
        return aiter(it) # type: ignore
    except TypeError:
        return to_aiter(cast(Iterable[T], it), threaded=threaded)


@asynccontextmanager
async def to_async_cm(cm, /):
    if isinstance(cm, AbstractContextManager):
        with cm as o:
            yield o
    else:
        yield cm


def ensure_async_cm(cm, /) -> AsyncContextManager:
    if isinstance(cm, AbstractAsyncContextManager):
        return cm
    return to_async_cm(cm)


async def coroutine[Y, R](gen: Generator[Y, None | Y, R], /) -> Coroutine[Y, None | Y, R]:
    send  = gen.send
    throw = gen.throw
    try:
        value = None
        while True:
            try:
                if isawaitable(value):
                    value = await value
                else:
                    await sleep(0)
            except BaseException as e:
                value = throw(e)
            else:
                value = send(value)
    except StopIteration as e:
        value = e.value
        if isawaitable(value):
            value = await value
        return value


@decorated
def coroutinefunction[**Args, Y, R](
    func: Callable[Args, Generator[Y, None | Y, R]], 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
):
    return coroutine(func(*args, **kwds))


@decorated
def as_thread[**Args, T](
    func: Callable[Args, T], 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
):
    def wrapfunc(*args, **kwds):
        try:
            return func(*args, **kwds)
        except StopIteration as e:
            raise StopAsyncIteration from e
    return to_thread(wrapfunc, *args, **kwds)

