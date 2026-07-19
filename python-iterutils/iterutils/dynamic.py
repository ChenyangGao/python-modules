#!/usr/bin/env python3
# encoding: utf-8

__all__ = ["dynamic_async", "dynamic_async_iter"]

from asyncio import AbstractEventLoop
from collections.abc import AsyncIterable, Callable, Iterable
from inspect import isawaitable

from argtools import has_keyword_arg
from asynctools import (
    ensure_async, in_async, iter_async, run_async, to_coroutine, 
    to_aiter, 
)
from decotools import optional


@optional
def dynamic_async(
    func: Callable, 
    /, 
    threaded: bool = False, 
    async_: None | bool = None, 
    loop: None | AbstractEventLoop = None, 
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
                    r = to_coroutine(r)
        else:
            r = func(*args, **kwds)
            if isawaitable(r):
                r = run_async(r, loop)
        return r
    return wrapper


@optional
def dynamic_async_iter(
    func: Callable[..., Iterable] | Callable[..., AsyncIterable], 
    /, 
    threaded: bool = False, 
    async_: None | bool = None, 
    loop: None | AbstractEventLoop = None, 
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
                it = to_aiter(it, threaded=threaded)
        elif isinstance(it, AsyncIterable):
            it = iter_async(it, loop)
        return it
    return wrapper

