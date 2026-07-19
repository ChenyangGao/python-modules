#!/usr/bin/env python3
# encoding: utf-8

__all__ = [
    "map", "filter", "reduce", "zip", "chain", "chain_from_iterable", 
    "chunked", "foreach", "through", "flatten", "collect", 
    "async_group_collect", "iter_unique", "async_iter_unique", 
    "wrap_iter", "wrap_aiter", "peek_iter", "peek_aiter", 
    "acc_step", "cut_iter", "context", "backgroud_loop", "gen_startup", 
    "async_gen_startup", "do_iter", "do_aiter", "bfs_iter", "bfs_gen", 
]

from asyncio import create_task, sleep as async_sleep
from builtins import map as _map, filter as _filter, zip as _zip
from collections import defaultdict, deque
from collections.abc import (
    AsyncGenerator, AsyncIterable, AsyncIterator, Awaitable, Buffer,
    Callable, Collection, Container, Coroutine, Generator, Iterable, 
    Iterator, Mapping, MutableMapping, MutableSet, MutableSequence, 
    Sequence, ValuesView, 
)
from contextlib import asynccontextmanager, contextmanager, ExitStack, AsyncExitStack
from copy import copy
from functools import update_wrapper
from itertools import batched, chain as _chain, pairwise
from inspect import isawaitable, iscoroutinefunction
from sys import _getframe
from _thread import start_new_thread
from time import sleep, time
from types import FrameType
from typing import (
    cast, overload, Any, AsyncContextManager, ContextManager, Literal, 
)

from asynctools import (
    async_filter, async_map, async_reduce, async_zip, async_batched, 
    ensure_async, ensure_aiter, async_chain, async_chain_from_iterable, 
    async_collect, async_foreach, async_through, 
)
from texttools import format_time
from undefined import undefined


def _coalesce(vals, default=None):
    for val in vals:
        if val is not None:
            return val
    return default


@overload
def _get_async(back: int = 2, /, *, default: Literal[False] = False) -> bool:
    ...
@overload
def _get_async[T](back: int = 2, /, *, default: T) -> bool | T:
    ...
def _get_async[T](back: int = 2, /, *, default: Literal[False] | T = False) -> bool | T:
    """往上查找，从最近的调用栈的命名空间中获取 `async_` 的值
    """
    def iter_frams(f: None | FrameType = _getframe(back)):
        while f:
            yield f.f_locals.get("async_")
            f = f.f_back
    return _coalesce(iter_frams(), default)


def map(
    function: None | Callable, 
    iterable: Iterable | AsyncIterable, 
    /, 
    *iterables: Iterable | AsyncIterable, 
    threaded: bool = False, 
):
    """
    """
    if (
        threaded or
        iscoroutinefunction(function) or 
        isinstance(iterable, AsyncIterable) or 
        any(isinstance(i, AsyncIterable) for i in iterables)
    ):
        if function is None:
            if iterables:
                return async_zip(iterable, *iterables, threaded=threaded)
            elif threaded:
                return ensure_aiter(iterable, threaded=threaded)
            else:
                return iterable
        return async_map(function, iterable, *iterables)
    if function is None:
        if iterables:
            return _zip(iterable, *iterables)
        else:
            return iterable
    return _map(function, iterable, *iterables)


def filter(
    function: None | Callable, 
    iterable: Iterable | AsyncIterable, 
    /, 
    threaded: bool = False, 
):
    """
    """
    if threaded or iscoroutinefunction(function) or isinstance(iterable, AsyncIterable):
        return async_filter(function, iterable, threaded=threaded)
    return _filter(function, iterable)


def reduce(
    function: Callable, 
    iterable: Iterable | AsyncIterable, 
    initial: Any = undefined, 
    /, 
    threaded: bool = False, 
):
    """
    """
    if threaded or iscoroutinefunction(function) or isinstance(iterable, AsyncIterable):
        return async_reduce(function, iterable, initial, threaded=threaded)
    from functools import reduce
    if initial is undefined:
        return reduce(function, iterable)
    return reduce(function, iterable, initial)


def zip(
    iterable: Iterable | AsyncIterable, 
    /, 
    *iterables: Iterable | AsyncIterable, 
    threaded: bool = False, 
):
    """
    """
    if (not threaded and 
        isinstance(iterable, Iterable) and 
        all(isinstance(it, Iterable) for it in iterables)
    ):
        return _zip(iterable, *iterables)
    return async_zip(iterable, *iterables, threaded=threaded)


@overload
def chain[T](
    iterable: Iterable[T], 
    /, 
    *iterables: Iterable[T], 
    threaded: Literal[False] = False, 
) -> Iterator[T]:
    ...
@overload
def chain[T](
    iterable: Iterable[T], 
    /, 
    *iterables: Iterable[T] | AsyncIterable[T], 
    threaded: Literal[True], 
) -> AsyncIterator[T]:
    ...
@overload
def chain[T](
    iterable: AsyncIterable[T], 
    /, 
    *iterables: Iterable[T] | AsyncIterable[T], 
    threaded: Literal[False, True] = False, 
) -> AsyncIterator[T]:
    ...
def chain[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    *iterables: Iterable[T] | AsyncIterable[T], 
    threaded: Literal[False, True] = False, 
) -> Iterator[T] | AsyncIterator[T]:
    if (not threaded and 
        isinstance(iterable, Iterable) and 
        all(isinstance(it, Iterable) for it in iterables)
    ):
        return _chain(iterable, *iterables) # type: ignore
    return async_chain(iterable, *iterables, threaded=threaded)


@overload
def chain_from_iterable[T](
    iterables: Iterable[Iterable[T]], 
    threaded: bool = False, 
    *, 
    async_: Literal[False] = False, 
) -> Iterator[T]:
    ...
@overload
def chain_from_iterable[T](
    iterables: (
        AsyncIterable[Iterable[T]] | 
        AsyncIterable[AsyncIterable[T]] | 
        AsyncIterable[Iterable[T] | AsyncIterable[T]]
    ), 
    threaded: bool = False, 
    *, 
    async_: bool = False, 
) -> AsyncIterator[T]:
    ...
@overload
def chain_from_iterable[T](
    iterables: (
        Iterable[Iterable[T]] | 
        Iterable[AsyncIterable[T]] | 
        Iterable[Iterable[T] | AsyncIterable[T]] | 
        AsyncIterable[Iterable[T]] | 
        AsyncIterable[AsyncIterable[T]] | 
        AsyncIterable[Iterable[T] | AsyncIterable[T]]
    ), 
    threaded: bool = False, 
    *, 
    async_: Literal[True], 
) -> AsyncIterator[T]:
    ...
def chain_from_iterable[T](
    iterables: (
        Iterable[Iterable[T]] | 
        Iterable[AsyncIterable[T]] | 
        Iterable[Iterable[T] | AsyncIterable[T]] | 
        AsyncIterable[Iterable[T]] | 
        AsyncIterable[AsyncIterable[T]] | 
        AsyncIterable[Iterable[T] | AsyncIterable[T]]
    ), 
    threaded: bool = False, 
    *, 
    async_: Literal[False, True] = False, 
) -> Iterator[T] | AsyncIterator[T]:
    if async_ or threaded:
        return async_chain_from_iterable(iterables, threaded=threaded)
    return _chain.from_iterable(iterables) # type: ignore

setattr(chain, "from_iterable", chain_from_iterable)


@overload
def chunked[T](
    iterable: Iterable[T], 
    n: int = 1, 
    /, 
    *, 
    threaded: Literal[False] = False, 
) -> Iterator[Sequence[T]]:
    ...
@overload
def chunked[T](
    iterable: Iterable[T], 
    n: int = 1, 
    /, 
    *, 
    threaded: Literal[True], 
) -> Iterator[Sequence[T]]:
    ...
@overload
def chunked[T](
    iterable: AsyncIterable[T], 
    n: int = 1, 
    /, 
    *, 
    threaded: Literal[False, True] = False, 
) -> AsyncIterator[Sequence[T]]:
    ...
def chunked[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    n: int = 1, 
    /, 
    *, 
    threaded: Literal[False, True] = False, 
) -> Iterator[Sequence[T]] | AsyncIterator[Sequence[T]]:
    """
    """
    if n < 0:
        n = 1
    if isinstance(iterable, Sequence):
        if n == 1:
            return ((e,) for e in iterable)
        return (iterable[i:j] for i, j in pairwise(range(0, len(iterable)+n, n)))
    elif not threaded and isinstance(iterable, Iterable):
        return batched(iterable, n)
    else:
        return async_batched(iterable, n, threaded=threaded)


def foreach(
    function: Callable, 
    iterable: Iterable | AsyncIterable, 
    /, 
    *iterables: Iterable | AsyncIterable, 
    default = None, 
    threaded: bool = False, 
):
    if (threaded or 
        isinstance(iterable, AsyncIterable) or 
        any(isinstance(it, AsyncIterable) for it in iterables)
    ):
        return async_foreach(
            function, 
            iterable, 
            *iterables, 
            default=default, 
            threaded=threaded, 
        )
    r = default
    if iterables:
        for args in _zip(iterable, *iterables):
            r = function(*args)
    else:
        for arg in iterable:
            r = function(arg)
    return r


def through(
    iterable: Iterable | AsyncIterable, 
    /, 
    threaded: bool = False, 
):
    """
    """
    if threaded or isinstance(iterable, AsyncIterable):
        return async_through(iterable, threaded=threaded)
    for _ in iterable:
        pass


@overload
def flatten(
    iterable: Iterable, 
    /, 
    exclude_types: type | tuple[type, ...] = (Buffer, str), 
    *, 
    threaded: Literal[False] = False, 
) -> Iterator:
    ...
@overload
def flatten(
    iterable: Iterable, 
    /, 
    exclude_types: type | tuple[type, ...] = (Buffer, str), 
    *, 
    threaded: Literal[True], 
) -> Iterator:
    ...
@overload
def flatten(
    iterable: AsyncIterable, 
    /, 
    exclude_types: type | tuple[type, ...] = (Buffer, str), 
    *, 
    threaded: Literal[False, True] = False, 
) -> AsyncIterator:
    ...
def flatten(
    iterable: Iterable | AsyncIterable, 
    /, 
    exclude_types: type | tuple[type, ...] = (Buffer, str), 
    threaded: Literal[False, True] = False, 
) -> Iterator | AsyncIterator:
    """
    """
    if threaded or not isinstance(iterable, Iterable):
        return async_flatten(iterable, exclude_types, threaded=threaded)
    def gen(iterable):
        for e in iterable:
            if isinstance(e, (Iterable, AsyncIterable)) and not isinstance(e, exclude_types):
                yield from gen(e)
            else:
                yield e
    return gen(iterable)


async def async_flatten(
    iterable: Iterable | AsyncIterable, 
    /, 
    exclude_types: type | tuple[type, ...] = (Buffer, str), 
    threaded: bool = False, 
) -> AsyncIterator:
    """
    """
    async for e in ensure_aiter(iterable, threaded=threaded):
        if isinstance(e, (Iterable, AsyncIterable)) and not isinstance(e, exclude_types):
            async for e in async_flatten(e, exclude_types, threaded=threaded):
                yield e
        else:
            yield e


@overload
def collect[K, V](
    iterable: Iterable[tuple[K, V]] | Mapping[K, V], 
    /, 
    rettype: Callable[[Iterable[tuple[K, V]]], MutableMapping[K, V]], 
    *, 
    threaded: Literal[False] = False, 
) -> MutableMapping[K, V]:
    ...
@overload
def collect[T](
    iterable: Iterable[T], 
    /, 
    rettype: Callable[[Iterable[T]], Collection[T]] = list, 
    *, 
    threaded: Literal[False] = False, 
) -> Collection[T]:
    ...
@overload
def collect[K, V](
    iterable: Iterable[tuple[K, V]] | Mapping[K, V], 
    /, 
    rettype: Callable[[Iterable[tuple[K, V]]], MutableMapping[K, V]], 
    *, 
    threaded: Literal[True], 
) -> Coroutine[Any, Any, MutableMapping[K, V]]:
    ...
@overload
def collect[T](
    iterable: Iterable[T], 
    /, 
    rettype: Callable[[Iterable[T]], Collection[T]] = list, 
    *, 
    threaded: Literal[True], 
) -> Coroutine[Any, Any, Collection[T]]:
    ...
@overload
def collect[K, V](
    iterable: AsyncIterable[tuple[K, V]], 
    /, 
    rettype: Callable[[Iterable[tuple[K, V]]], MutableMapping[K, V]], 
    *, 
    threaded: Literal[False, True] = False, 
) -> Coroutine[Any, Any, MutableMapping[K, V]]:
    ...
@overload
def collect[T](
    iterable: AsyncIterable[T], 
    /, 
    rettype: Callable[[Iterable[T]], Collection[T]] = list, 
    *, 
    threaded: Literal[False, True] = False, 
) -> Coroutine[Any, Any, Collection[T]]:
    ...
def collect(
    iterable: Iterable | AsyncIterable | Mapping, 
    /, 
    rettype: Callable[[Iterable], Collection] = list, 
    *, 
    threaded: Literal[False, True] = False, 
) -> Collection | Coroutine[Any, Any, Collection]:
    """
    """
    if threaded or not isinstance(iterable, Iterable):
        return async_collect(iterable, rettype, threaded=threaded)
    return rettype(iterable)


@overload
def group_collect[K, V, C: Container](
    iterable: Iterable[tuple[K, V]], 
    mapping: None = None, 
    factory: None | C | Callable[[], C] = None, 
    threaded: bool = False, 
) -> dict[K, C]:
    ...
@overload
def group_collect[K, V, C: Container, M: MutableMapping](
    iterable: Iterable[tuple[K, V]], 
    mapping: M, 
    factory: None | C | Callable[[], C] = None, 
    threaded: bool = False, 
) -> M:
    ...
@overload
def group_collect[K, V, C: Container](
    iterable: AsyncIterable[tuple[K, V]], 
    mapping: None = None, 
    factory: None | C | Callable[[], C] = None, 
    threaded: bool = False, 
) -> Coroutine[Any, Any, dict[K, C]]:
    ...
@overload
def group_collect[K, V, C: Container, M: MutableMapping](
    iterable: AsyncIterable[tuple[K, V]], 
    mapping: M, 
    factory: None | C | Callable[[], C] = None, 
    threaded: bool = False, 
) -> Coroutine[Any, Any, M]:
    ...
def group_collect[K, V, C: Container, M: MutableMapping](
    iterable: Iterable[tuple[K, V]] | AsyncIterable[tuple[K, V]], 
    mapping: None | M = None, 
    factory: None | C | Callable[[], C] = None, 
    threaded: bool = False, 
) -> dict[K, C] | M | Coroutine[Any, Any, dict[K, C]] | Coroutine[Any, Any, M]:
    """
    """
    if threaded or not isinstance(iterable, Iterable):
        return async_group_collect(iterable, mapping, factory, threaded=threaded)
    if factory is None:
        if isinstance(mapping, defaultdict):
            factory = mapping.default_factory
        elif mapping:
            factory = type(next(iter(ValuesView(mapping))))
        else:
            factory = cast(type[C], list)
    elif callable(factory):
        pass
    elif isinstance(factory, Container):
        factory = cast(Callable[[], C], lambda _obj=factory: copy(_obj))
    else:
        raise ValueError("can't determine factory")
    factory = cast(Callable[[], C], factory)
    if isinstance(factory, type):
        factory_type = factory
    else:
        factory_type = type(factory())
    if issubclass(factory_type, MutableSequence):
        add = getattr(factory_type, "append")
    else:
        add = getattr(factory_type, "add")
    if mapping is None:
        mapping = cast(M, {})
    for k, v in iterable:
        try:
            c = mapping[k]
        except LookupError:
            c = mapping[k] = factory()
        add(c, v)
    return mapping


@overload
async def async_group_collect[K, V, C: Container](
    iterable: Iterable[tuple[K, V]] | AsyncIterable[tuple[K, V]], 
    mapping: None = None, 
    factory: None | C | Callable[[], C] = None, 
    threaded: bool = False, 
) -> dict[K, C]:
    ...
@overload
async def async_group_collect[K, V, C: Container, M: MutableMapping](
    iterable: Iterable[tuple[K, V]] | AsyncIterable[tuple[K, V]], 
    mapping: M, 
    factory: None | C | Callable[[], C] = None, 
    threaded: bool = False, 
) -> M:
    ...
async def async_group_collect[K, V, C: Container, M: MutableMapping](
    iterable: Iterable[tuple[K, V]] | AsyncIterable[tuple[K, V]], 
    mapping: None | M = None, 
    factory: None | C | Callable[[], C] = None, 
    threaded: bool = False, 
) -> dict[K, C] | M:
    """
    """
    iterable = ensure_aiter(iterable, threaded=threaded)
    if factory is None:
        if isinstance(mapping, defaultdict):
            factory = mapping.default_factory
        elif mapping:
            factory = type(next(iter(ValuesView(mapping))))
        else:
            factory = cast(type[C], list)
    elif callable(factory):
        pass
    elif isinstance(factory, Container):
        factory = cast(Callable[[], C], lambda _obj=factory: copy(_obj))
    else:
        raise ValueError("can't determine factory")
    factory = cast(Callable[[], C], factory)
    if isinstance(factory, type):
        factory_type = factory
    else:
        factory_type = type(factory())
    if issubclass(factory_type, MutableSequence):
        add = getattr(factory_type, "append")
    else:
        add = getattr(factory_type, "add")
    if mapping is None:
        mapping = cast(M, {})
    async for k, v in iterable:
        try:
            c = mapping[k]
        except LookupError:
            c = mapping[k] = factory()
        add(c, v)
    return mapping


@overload
def iter_unique[T](
    iterable: Iterable[T], 
    /, 
    seen: None | MutableSet = None, 
    *, 
    threaded: Literal[False] = False, 
) -> Iterator[T]:
    ...
@overload
def iter_unique[T](
    iterable: Iterable[T], 
    /, 
    seen: None | MutableSet = None, 
    *, 
    threaded: Literal[True], 
) -> AsyncIterator[T]:
    ...
@overload
def iter_unique[T](
    iterable: AsyncIterable[T], 
    /, 
    seen: None | MutableSet = None, 
    *, 
    threaded: Literal[False, True] = False, 
) -> AsyncIterator[T]:
    ...
def iter_unique[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    seen: None | MutableSet = None, 
    threaded: Literal[False, True] = False, 
) -> Iterator[T] | AsyncIterator[T]:
    """
    """
    if threaded or not isinstance(iterable, Iterable):
        return async_iter_unique(iterable, seen, threaded=threaded)
    if seen is None:
        seen = set()
    def gen(iterable):
        add = seen.add
        for e in iterable:
            if e not in seen:
                yield e
                add(e)
    return gen(iterable)


async def async_iter_unique[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    seen: None | MutableSet = None, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    """
    """
    if seen is None:
        seen = set()
    add = seen.add
    async for e in ensure_aiter(iterable, threaded=threaded):
        if e not in seen:
            yield e
            add(e)


@overload
def wrap_iter[T](
    iterable: Iterable[T], 
    /, 
    callprev: None | Callable[[T], Any] = None, 
    callnext: None | Callable[[T], Any] = None, 
    *, 
    threaded: Literal[False] = False, 
) -> Iterator[T]:
    ...
@overload
def wrap_iter[T](
    iterable: Iterable[T], 
    /, 
    callprev: None | Callable[[T], Any] = None, 
    callnext: None | Callable[[T], Any] = None, 
    *, 
    threaded: Literal[True], 
) -> AsyncIterator[T]:
    ...
@overload
def wrap_iter[T](
    iterable: AsyncIterable[T], 
    /, 
    callprev: None | Callable[[T], Any] = None, 
    callnext: None | Callable[[T], Any] = None, 
    threaded: Literal[False, True] = False, 
) -> AsyncIterator[T]:
    ...
def wrap_iter[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    callprev: None | Callable[[T], Any] = None, 
    callnext: None | Callable[[T], Any] = None, 
    threaded: bool = False, 
) -> Iterator[T] | AsyncIterator[T]:
    """
    """
    if threaded or not isinstance(iterable, Iterable):
        return wrap_aiter(
            iterable, 
            callprev=callprev, 
            callnext=callnext, 
            threaded=threaded, 
        )
    if not callable(callprev):
        callprev = None
    if not callable(callnext):
        callnext = None
    def gen():
        for e in iterable:
            callprev and callprev(e)
            yield e
            callnext and callnext(e)
    return gen()


async def wrap_aiter[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    callprev: None | Callable[[T], Any] = None, 
    callnext: None | Callable[[T], Any] = None, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    """
    """
    callprev = ensure_async(callprev, threaded=threaded) if callable(callprev) else None
    callnext = ensure_async(callnext, threaded=threaded) if callable(callnext) else None
    async for e in ensure_aiter(iterable, threaded=threaded):
        callprev and await callprev(e)
        yield e
        callnext and await callnext(e)


@overload
def peek_iter[T](
    iterable: Iterable[T], 
    /, 
    threaded: Literal[False] = False, 
) -> None | Iterator[T]:
    ...
@overload
def peek_iter[T](
    iterable: Iterable[T], 
    /, 
    threaded: Literal[True], 
) -> Coroutine[Any, Any, None | AsyncIterator[T]]:
    ...
@overload
def peek_iter[T](
    iterable: AsyncIterable[T], 
    /, 
    threaded: Literal[False, True] = False, 
) -> Coroutine[Any, Any, None | AsyncIterator[T]]:
    ...
def peek_iter[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    threaded: Literal[False, True] = False, 
) -> None | Iterator[T] | Coroutine[Any, Any, None | AsyncIterator[T]]:
    if threaded or isinstance(iterable, AsyncIterable):
        return peek_aiter(iterable, threaded=threaded)
    try:
        it = iter(iterable)
        first = next(it)
        return chain((first,), it)
    except StopIteration:
        return None


async def peek_aiter[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    threaded: Literal[False, True] = False, 
) -> None | AsyncIterator[T]:
    try:
        it = ensure_aiter(iterable, threaded=threaded)
        first = await anext(it)
        return async_chain((first,), it)
    except StopAsyncIteration:
        return None


def acc_step(
    start: int, 
    stop: None | int = None, 
    step: int = 1, 
) -> Iterator[tuple[int, int, int]]:
    """
    """
    if stop is None:
        start, stop = 0, start
    for i in range(start + step, stop, step):
        yield start, (start := i), step
    if start != stop:
        yield start, stop, stop - start


def cut_iter(
    start: int, 
    stop: None | int = None, 
    step: int = 1, 
) -> Iterator[tuple[int, int]]:
    """
    """
    if stop is None:
        start, stop = 0, start
    for start in range(start + step, stop, step):
        yield start, step
    if start != stop:
        yield stop, stop - start


@overload
def context[T](
    func: Callable[..., T], 
    *ctxs: ContextManager, 
    async_: Literal[False], 
) -> T:
    ...
@overload
def context[T](
    func: Callable[..., T] | Callable[..., Awaitable[T]], 
    *ctxs: ContextManager | AsyncContextManager, 
    async_: Literal[True], 
) -> Coroutine[Any, Any, T]:
    ...
@overload
def context[T](
    func: Callable[..., T] | Callable[..., Awaitable[T]], 
    *ctxs: ContextManager | AsyncContextManager, 
    async_: None = None, 
) -> T | Coroutine[Any, Any, T]:
    ...
def context[T](
    func: Callable[..., T] | Callable[..., Awaitable[T]], 
    *ctxs: ContextManager | AsyncContextManager, 
    async_: None | Literal[False, True] = None, 
) -> T | Coroutine[Any, Any, T]:
    """
    """
    if async_ is None:
        if iscoroutinefunction(func):
            async_ = True
        else:
            async_ = _get_async()
    if async_:
        async def call():
            args: list = []
            add_arg = args.append
            with ExitStack() as stack:
                async with AsyncExitStack() as async_stack:
                    enter = stack.enter_context
                    async_enter = async_stack.enter_async_context
                    for ctx in ctxs:
                        if isinstance(ctx, AsyncContextManager):
                            add_arg(await async_enter(ctx))
                        else:
                            add_arg(enter(ctx))
                    ret = func(*args)
                    if isawaitable(ret):
                        ret = await cast(Awaitable, ret)
                    return ret
        return call()
    else:
        with ExitStack() as stack:
            return func(*map(stack.enter_context, ctxs)) # type: ignore


@overload
def backgroud_loop(
    call: None | Callable = None, 
    /, 
    interval: int | float = 0.05, 
    *, 
    async_: Literal[False], 
) -> ContextManager:
    ...
@overload
def backgroud_loop(
    call: None | Callable = None, 
    /, 
    interval: int | float = 0.05, 
    *, 
    async_: Literal[True], 
) -> AsyncContextManager:
    ...
@overload
def backgroud_loop(
    call: None | Callable = None, 
    /, 
    interval: int | float = 0.05, 
    *, 
    async_: None = None, 
) -> ContextManager | AsyncContextManager:
    ...
def backgroud_loop(
    call: None | Callable = None, 
    /, 
    interval: int | float = 0.05, 
    *, 
    async_: None | Literal[False, True] = None, 
) -> ContextManager | AsyncContextManager:
    """
    """
    if async_ is None:
        if iscoroutinefunction(call):
            async_ = True
        else:
            async_ = _get_async()
    use_default_call = not callable(call)
    if use_default_call:
        start = time()
        def call():
            print(f"\r\x1b[K{format_time(time() - start)}", end="")
    def run():
        while running:
            try:
                yield call
            except Exception:
                pass
            if interval > 0:
                if async_:
                    yield async_sleep(interval)
                else:
                    sleep(interval)
    running = True
    if async_:
        @asynccontextmanager
        async def actx():
            nonlocal running
            try:
                task = create_task(run())
                yield task
            finally:
                running = False
                task.cancel()
                if use_default_call:
                    print("\r\x1b[K", end="")
        return actx()
    else:
        @contextmanager
        def ctx():
            nonlocal running
            try:
                yield start_new_thread(run, ())
            finally:
                running = False
                if use_default_call:
                    print("\r\x1b[K", end="")
        return ctx()


def gen_startup[**Args, G: Generator](func: Callable[Args, G], /):
    def wrapper(*args: Args.args, **kwds: Args.kwargs) -> G:
        gen = func(*args, **kwds)
        next(gen)
        return gen
    return update_wrapper(wrapper, func)


def async_gen_startup[**Args, G: AsyncGenerator](func: Callable[Args, G], /):
    async def wrapper(*args: Args.args, **kwds: Args.kwargs) -> G:
        gen = func(*args, **kwds)
        await anext(gen)
        return gen
    return update_wrapper(wrapper, func)


def do_iter[T](
    func: Callable[[], T] | Iterable[T], 
    /, 
    sentinel=undefined, 
    sentinel_excs: type[BaseException] | tuple[type[BaseException], ...] = (), 
) -> Iterator[T]:
    try:
        yield from iter(func if callable(func) else iter(func).__next__, sentinel)
    except sentinel_excs:
        pass


async def do_aiter[T](
    func: Callable[[], T] | Callable[[], Awaitable[T]] | Iterable[T] | AsyncIterable[T],  
    /, 
    sentinel=undefined, 
    sentinel_excs: type[BaseException] | tuple[type[BaseException], ...] = (), 
) -> AsyncIterator[T]:
    if callable(func):
        func = ensure_async(func)
    else:
        func = ensure_aiter(func).__anext__
    try:
        while True:
            v = await func()
            if v is sentinel:
                break
            yield v
    except StopAsyncIteration:
        pass
    except sentinel_excs:
        pass


def bfs_iter[T](*initials: T) -> tuple[Iterator[T], Callable[[T], None]]:
    dq = deque(initials)
    return do_iter(dq.popleft, sentinel_excs=IndexError), dq.append


@gen_startup
def bfs_gen[T](*initials) -> Generator[None | T, T | None, None]:
    dq = deque(initials)
    push, pop = dq.append, dq.popleft
    try:
        p = yield None
        while True:
            p = yield (pop() if p is None else push(p))
    except IndexError:
        pass

# TODO: 这个模块添加了很多不必要的函数，需要进行移除
