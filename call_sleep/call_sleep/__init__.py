#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 1)
__all__ = ["call_sleep", "cm_sleep"]

from asyncio import sleep as async_sleep, to_thread, Lock as AsyncLock
from collections.abc import Callable
from contextlib import (
    asynccontextmanager, contextmanager, AbstractAsyncContextManager, AbstractContextManager, 
)
from inspect import iscoroutinefunction
from sys import exc_info
from threading import Lock
from time import sleep, perf_counter
from typing import cast, AsyncContextManager, ContextManager

from asynctools import run_async
from decotools import optional


def _set_method(o, /):
    def set(m, /):
        setattr(o, m.__name__, m)
        return m
    return set


@contextmanager
def cm_as_sync(cm: AsyncContextManager, /):
    o = run_async(cm.__aenter__())
    try:
        yield o
    finally:
        run_async(cm.__aexit__(*exc_info()))


@asynccontextmanager
async def cm_as_async(cm: ContextManager, /):
    o = await to_thread(cm.__enter__)
    try:
        yield o
    finally:
        cm.__exit__(*exc_info())


@optional
def call_sleep(
    func: Callable, 
    /, 
    duration: float = 1, 
    con_count: int = 1, 
    lock: None | AsyncContextManager | ContextManager = None, 
    async_: bool = False, 
):
    if iscoroutinefunction(func):
        async_ = False
    last_call_t: float = 0
    running = True
    wrapper: Callable
    count = 0
    if async_:
        if lock is None:
            lock = AsyncLock()
        elif isinstance(lock, AbstractContextManager):
            lock = cm_as_async(lock)
        async def wrapper(*args, **kwds):
            nonlocal last_call_t, count
            if not running:
                raise RuntimeError
            if duration > 0 and con_count > 0:
                async with cast(AsyncContextManager, lock):
                    if count == 0:
                        delta = last_call_t + duration - perf_counter()
                        if delta > 0:
                            await async_sleep(delta)
                        if not running:
                            raise RuntimeError
                        last_call_t = perf_counter()
                    count = (count + 1) % con_count
            return await func(*args, **kwds)
    else:
        if lock is None:
            lock = Lock()
        elif isinstance(lock, AbstractAsyncContextManager):
            lock = cm_as_sync(lock)
        def wrapper(*args, **kwds):
            nonlocal last_call_t, count
            if not running:
                raise RuntimeError
            if duration > 0 and con_count > 0:
                with cast(ContextManager, lock):
                    if count == 0:
                        delta = last_call_t + duration - perf_counter()
                        if delta > 0:
                            sleep(delta)
                        if not running:
                            raise RuntimeError
                        last_call_t = perf_counter()
                    count = (count + 1) % con_count
            return func(*args, **kwds)
    @_set_method(wrapper)
    def set_duration(d: float, /):
        nonlocal duration
        duration = d
    @_set_method(wrapper)
    def close():
        nonlocal running
        running = False
    return wrapper


def cm_sleep(
    duration: float = 1, 
    lock: None | AsyncContextManager | ContextManager = None, 
    async_: bool = False, 
):
    last_call_t: float = 0
    if async_:
        if lock is None:
            lock = AsyncLock()
        elif isinstance(lock, AbstractContextManager):
            lock = cm_as_async(lock)
        @asynccontextmanager
        async def acm():
            nonlocal last_call_t
            if duration > 0:
                async with cast(AsyncContextManager, lock):
                    delta = last_call_t + duration - perf_counter()
                    if delta > 0:
                        await async_sleep(delta)
                    last_call_t = perf_counter()
            yield
        return acm()
    else:
        if lock is None:
            lock = Lock()
        elif isinstance(lock, AbstractAsyncContextManager):
            lock = cm_as_sync(lock)
        @contextmanager
        def cm():
            nonlocal last_call_t
            if duration > 0:
                with cast(ContextManager, lock):
                    delta = last_call_t + duration - perf_counter()
                    if delta > 0:
                        sleep(delta)
                    last_call_t = perf_counter()
            yield
        return cm()

# TODO: 再提供一个函数，让迭代器的第一次迭代时，才会被锁时间，主要用于生成器
