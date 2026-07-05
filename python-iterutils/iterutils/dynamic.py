#!/usr/bin/env python3
# encoding: utf-8

__all__ = ["dynamic_async", "dynamic_async_iter"]

from asyncio import run as asyncio_run, to_thread
from collections.abc import (
    AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, Iterator, 
)
from inspect import isawaitable

from argtools import has_keyword_arg
from asynctools import ensure_async, in_async
from decotools import optional


async def _as_async[T](v: T, /) -> T:
    return v


async def _as_async_iter[T](
    it: Iterable[T], 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    if threaded:
        call = iter(it).__next__
        try:
            while True:
                yield await to_thread(call)
        except StopIteration:
            pass
    else:
        for e in it:
            yield e


def _as_iter[T](
    it: AsyncIterable[T], 
    /, 
    run: Callable = asyncio_run, 
) -> Iterator[T]:
    try:
        call = aiter(it).__anext__
        while True:
            yield run(call())
    except StopAsyncIteration:
        pass


@optional
def dynamic_async[T](
    func: Callable, 
    /, 
    run: Callable = asyncio_run, 
    threaded: bool = False, 
    async_: None | bool = None, 
):
    has_async_arg = has_keyword_arg(func, "async_")
    def wrapper(*args, async_: None | bool = async_, **kwds):
        if async_ is None:
            async_ = in_async()
        if has_async_arg:
            kwds["async_"] = async_
        if async_:
            if threaded:
                r = ensure_async(func, threaded=threaded)(*args, **kwds)
            else:
                r = func(*args, **kwds)
                if not isawaitable(r):
                    r = _as_async(r)
        else:
            r = func(*args, **kwds)
            if isawaitable(r):
                r = run(r)
        return r
    return wrapper


@optional
def dynamic_async_iter[T](
    func: Callable[..., Iterable] | Callable[..., AsyncIterable], 
    /, 
    run: Callable = asyncio_run, 
    threaded: bool = False, 
    async_: None | bool = None, 
):
    has_async_arg = has_keyword_arg(func, "async_")
    def wrapper(*args, async_: None | bool = async_, **kwds):
        if async_ is None:
            async_ = in_async()
        if has_async_arg:
            kwds["async_"] = async_
        it = func(*args, **kwds)
        if async_:
            if not isinstance(it, AsyncIterable):
                it = _as_async_iter(it, threaded=threaded)
        elif isinstance(it, AsyncIterable):
            it = _as_iter(it, run)
        return it
    return wrapper

