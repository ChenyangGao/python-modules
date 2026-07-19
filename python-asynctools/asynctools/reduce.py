#!/usr/bin/env python3
# encoding: utf-8

__all__ = [
    "async_all", "async_any", "async_reduce", "async_sum", "async_max", 
    "async_min", "async_through", "async_foreach", "async_star_foreach", 
    "async_collect", 
]

from collections.abc import (
    Awaitable, Callable, Coroutine, Iterable, AsyncIterable, Collection, 
    ItemsView, Mapping, MutableMapping, MutableSequence, MutableSet, 
)
from operator import add
from typing import cast, overload, Any

from undefined import undefined, Undefined

from .basic import ensure_aiter, ensure_async
from .type import SupportsBool, SupportsAdd, SupportsRAdd


async def async_all[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    predicate: None | Callable[[T], Awaitable[SupportsBool]] | Callable[[T], SupportsBool], 
    threaded: bool = False, 
) -> bool:
    if predicate is None:
        async for e in ensure_aiter(iterable, threaded=threaded):
            if not e:
                return False
    else:
        predicate = ensure_async(predicate, threaded=threaded)
        async for e in ensure_aiter(iterable, threaded=threaded):
            if not await predicate(e):
                return False
    return True


async def async_any[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    predicate: None | Callable[[T], SupportsBool] | Callable[[T], Awaitable[SupportsBool]], 
    threaded: bool = False, 
) -> bool:
    if predicate is None:
        async for e in ensure_aiter(iterable, threaded=threaded):
            if e:
                return True
    else:
        predicate = ensure_async(predicate, threaded=threaded)
        async for e in ensure_aiter(iterable, threaded=threaded):
            if await predicate(e):
                return True
    return False


async def async_reduce[T](
    function: Callable[[T, T], Awaitable[T]] | Callable[[T, T], T], 
    iterable: Iterable[T] | AsyncIterable[T], 
    initial: Undefined | T = undefined, 
    /, 
    threaded: bool = False, 
) -> T:
    iterator = ensure_aiter(iterable, threaded=threaded)
    if initial is undefined:
        try:
            initial = await iterator.__anext__()
        except StopAsyncIteration:
            raise TypeError("reduce() on empty iterable without initial value") from None
    initial = cast(T, initial)
    call = ensure_async(function, threaded=threaded)
    async for e in iterator:
        initial = await call(initial, e)
    return initial


def async_sum[T: SupportsAdd | SupportsRAdd](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    start: int | T = 0
) -> Coroutine[Any, Any, T]:
    return async_reduce(add, iterable, start)


def async_max[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
) -> Coroutine[Any, Any, T]:
    return async_reduce(lambda x, y: y if x < y else x, iterable)


def async_min[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
) -> Coroutine[Any, Any, T]:
    return async_reduce(lambda x, y: y if x > y else x, iterable)


async def async_through(
    iterable: Iterable | AsyncIterable, 
    /, 
    threaded: bool = False, 
):
    if threaded or isinstance(iterable, AsyncIterable):
        async for _ in ensure_aiter(iterable, threaded=threaded):
            pass
    else:
        for _ in iterable:
            pass


async def async_foreach[T, R](
    function: Callable[..., Awaitable[T]] | Callable[..., T], 
    iterable: Iterable | AsyncIterable, 
    /, 
    *iterables: Iterable | AsyncIterable, 
    default: None | R = None, 
    threaded: bool = False, 
) -> None | T | R:
    function = ensure_async(function, threaded=threaded)
    r: Any = default
    if iterables:
        from .iter import async_zip
        async for args in async_zip(iterable, *iterables, threaded=threaded):
            r = await function(*args)
    else:
        async for arg in ensure_aiter(iterable, threaded=threaded):
            r = await function(arg)
    return r


async def async_star_foreach[T, R](
    function: Callable[..., Awaitable[T]] | Callable[..., T], 
    iterable: Iterable | AsyncIterable, 
    /, 
    default: None | R = None, 
    threaded: bool = False, 
) -> None | T | R:
    function = ensure_async(function, threaded=threaded)
    r: Any = default
    async for args in ensure_aiter(iterable, threaded=threaded):
        r = await function(*args)
    return r


@overload
async def async_collect[K, V](
    iterable: Iterable[tuple[K, V]] | AsyncIterable[tuple[K, V]] | MutableMapping[K, V], 
    /, 
    rettype: Callable[[Iterable[tuple[K, V]]], MutableMapping[K, V]], 
    threaded: bool = False, 
) -> MutableMapping[K, V]:
    ...
@overload
async def async_collect[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    rettype: Callable[[Iterable[T]], Collection[T]] = list,  
    threaded: bool = False, 
) -> Collection[T]:
    ...
async def async_collect(
    iterable: Iterable | AsyncIterable, 
    /, 
    rettype: Callable[[Iterable], Collection] = list, 
    threaded: bool = False, 
) -> Collection:
    if isinstance(iterable, Mapping):
        try:
            iterable = iterable.items()
        except (AttributeError, TypeError):
            iterable = ItemsView(cast(Mapping, iterable))
    if isinstance(iterable, Iterable) and not threaded:
        return rettype(iterable)
    iterator = ensure_aiter(iterable, threaded=threaded)
    if isinstance(rettype, type):
        if issubclass(rettype, MutableSequence):
            if rettype is list:
                return [e async for e in iterator]
            ls = rettype()
            await async_foreach(ls.append, iterator)
            return ls
        elif issubclass(rettype, MutableSet):
            if rettype is set:
                return {e async for e in iterator}
            st = rettype()
            await async_foreach(st.add, iterator)
            return st
        elif issubclass(rettype, MutableMapping):
            if rettype is dict:
                return {k: v async for k, v in iterator}
            dt = rettype()
            await async_star_foreach(dt.__setitem__, iterator)
            return dt
    return cast(Callable, rettype)([e async for e in iterator])

