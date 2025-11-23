#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 2, 10)
__all__ = [
    "Yield", "YieldFrom", "GenStep", "GenStepIter", "iterable", 
    "async_iterable", "run_gen_step", "run_gen_step_iter", 
    "as_gen_step", "as_gen_step_iter", "split_cm", "with_iter_next", 
    "map", "filter", "reduce", "zip", "chain", "chain_from_iterable", 
    "chunked", "foreach", "async_foreach", "through", "async_through", 
    "flatten", "async_flatten", "collect", "async_collect", 
    "group_collect", "async_group_collect", "iter_unique", 
    "async_iter_unique", "wrap_iter", "wrap_aiter", "peek_iter", "peek_aiter", 
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
from contextlib import (
    asynccontextmanager, contextmanager, ExitStack, AsyncExitStack, 
    AbstractContextManager, AbstractAsyncContextManager, 
)
from copy import copy
from dataclasses import dataclass
from functools import update_wrapper
from itertools import batched, chain as _chain, pairwise
from inspect import iscoroutinefunction, signature
from sys import _getframe, exc_info
from _thread import start_new_thread
from time import sleep, time
from types import FrameType
from typing import (
    cast, overload, Any, AsyncContextManager, ContextManager, Literal, 
)

from asynctools import (
    async_filter, async_map, async_reduce, async_zip, async_batched, 
    ensure_async, ensure_aiter, async_chain, collect as async_collect, 
)
from texttools import format_time
from undefined import undefined


@dataclass(slots=True, frozen=True, unsafe_hash=True)
class Yield:
    """专供 `run_gen_step_iter`，说明值需要 yield 给用户
    """
    value: Any


@dataclass(slots=True, frozen=True, unsafe_hash=True)
class YieldFrom:
    """专供 `run_gen_step_iter`，说明值需要解包后逐个 yield 给用户
    """
    value: Any


@dataclass(slots=True, frozen=True, unsafe_hash=True)
class GenStep:
    """专供 `run_gen_step` 和 `run_gen_step_iter`，说明值需要进一步提取
    """
    value: Generator
    max_depth: int = 0


@dataclass(slots=True, frozen=True, unsafe_hash=True)
class GenStepIter:
    """专供 `run_gen_step_iter`，说明值需要进一步提取
    """
    value: Generator
    max_depth: int = 0


iterable = Iterable.__instancecheck__
async_iterable = AsyncIterable.__instancecheck__
isawaitable = Awaitable.__instancecheck__


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


def _run_gen_step(
    gen: Generator, 
    /, 
    drive_generator: int = 0, 
):
    send  = gen.send
    throw = gen.throw
    dgen  = drive_generator if drive_generator < 0 else drive_generator - 1
    try:
        value: Any = send(None)
        while True:
            try:
                if isinstance(value, GenStep):
                    value = _run_gen_step(value.value, value.max_depth)
                elif drive_generator and isinstance(value, Generator):
                    value = _run_gen_step(value, dgen)
            except BaseException as e:
                value = throw(e)
            else:
                value = send(value)
    except StopIteration as e:
        value = e.value
        if isinstance(value, GenStep):
            value = _run_gen_step(value.value, value.max_depth)
        elif drive_generator and isinstance(value, Generator):
            value = _run_gen_step(value, dgen)
        return value
    finally:
        gen.close()


async def _run_gen_step_async(
    gen: Generator, 
    /, 
    drive_generator: int = 0, 
):
    send  = gen.send
    throw = gen.throw
    dgen = drive_generator if drive_generator < 0 else drive_generator - 1
    try:
        value: Any = send(None)
        while True:
            try:
                if isawaitable(value):
                    value = await value
                elif isinstance(value, GenStep):
                    value = await _run_gen_step_async(value.value, value.max_depth)
                elif drive_generator and isinstance(value, Generator):
                    value = await _run_gen_step_async(value, dgen)
            except BaseException as e:
                value = throw(e)
            else:
                value = send(value)
    except StopIteration as e:
        value = e.value
        if isawaitable(value):
            value = await value
        elif isinstance(value, GenStep):
            value = await _run_gen_step_async(value.value, value.max_depth)
        elif drive_generator and isinstance(value, Generator):
            value = await _run_gen_step_async(value, dgen)
        return value
    finally:
        gen.close()


def run_gen_step[**Args](
    gen_step: Generator | Callable[Args, Generator], 
    async_: None | Literal[False, True] = None, 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
):
    """驱动生成器运行，并返回其结果
    """
    if async_ is None:
        async_ = _get_async()
    if not isinstance(gen_step, Generator):
        params = signature(gen_step).parameters
        if ((param := params.get("async_")) and 
            (param.kind in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY))
        ):
            kwds.setdefault("async_", async_)
        gen_step = gen_step(*args, **kwds)
    if async_:
        return _run_gen_step_async(gen_step)
    else:
        return _run_gen_step(gen_step)


def _run_gen_step_iter(
    gen: Generator, 
    /, 
    drive_generator: int = 0, 
) -> Iterator:
    send  = gen.send
    throw = gen.throw
    dgen  = drive_generator if drive_generator < 0 else drive_generator - 1
    try:
        value: Any = send(None)
        while True:
            try:
                val_type = type(value)
                if issubclass(val_type, (Yield, YieldFrom)):
                    value = value.value
                if isinstance(value, GenStepIter):
                    yield from _run_gen_step_iter(value.value, value.max_depth)
                else:
                    if isinstance(value, GenStep):
                        value = _run_gen_step(value.value, value.max_depth)
                    elif drive_generator and isinstance(value, Generator):
                        value = _run_gen_step(value, dgen)
                    if val_type is Yield:
                        yield value
                    elif val_type is YieldFrom:
                        yield from value
            except BaseException as e:
                value = throw(e)
            else:
                value = send(value)
    except StopIteration as e:
        value = e.value
        val_type = type(value)
        if issubclass(val_type, (Yield, YieldFrom)):
            value = value.value
        elif isinstance(value, GenStepIter):
            yield from _run_gen_step_iter(value.value, value.max_depth)
        elif isinstance(value, GenStep):
            value = _run_gen_step(value.value, value.max_depth)
        elif drive_generator and isinstance(value, Generator):
            value = _run_gen_step(value, dgen)
        if val_type is Yield:
            yield value
        elif val_type is YieldFrom:
            yield from value
    finally:
        gen.close()


async def _run_gen_step_async_iter(
    gen: Generator, 
    /, 
    drive_generator: int = 0, 
) -> AsyncIterator:
    send  = gen.send
    throw = gen.throw
    dgen = drive_generator if drive_generator < 0 else drive_generator - 1
    try:
        value: Any = send(None)
        while True:
            try:
                val_type = type(value)
                if issubclass(val_type, (Yield, YieldFrom)):
                    value = value.value
                if isawaitable(value):
                    value = await value
                elif isinstance(value, GenStepIter):
                    async for el in _run_gen_step_async_iter(value.value, value.max_depth):
                        yield el
                elif isinstance(value, GenStep):
                    value = await _run_gen_step_async(value.value, value.max_depth)
                elif drive_generator and isinstance(value, Generator):
                    value = await _run_gen_step_async(value, dgen)
                if val_type is Yield:
                    yield value
                elif val_type is YieldFrom:
                    if isinstance(value, AsyncIterable):
                        async for el in value:
                            yield el
                    else:
                        for el in value:
                            yield el
            except BaseException as e:
                value = throw(e)
            else:
                value = send(value)
    except StopIteration as e:
        value = e.value
        val_type = type(value)
        if issubclass(val_type, (Yield, YieldFrom)):
            value = value.value
        if isawaitable(value):
            value = await value
        if isinstance(value, GenStepIter):
            async for el in _run_gen_step_async_iter(value.value, value.max_depth):
                yield el
        else:
            if isinstance(value, GenStep):
                value = await _run_gen_step_async(value.value, value.max_depth)
            elif drive_generator and isinstance(value, Generator):
                value = await _run_gen_step_async(value, dgen)
            if val_type is Yield:
                yield value
            elif val_type is YieldFrom:
                if isinstance(value, AsyncIterable):
                    async for el in value:
                        yield el
                else:
                    for el in value:
                        yield el
    finally:
        gen.close()


@overload
def run_gen_step_iter[**Args](
    gen_step: Generator | Callable[Args, Generator], 
    async_: None = None, 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
) -> Iterator | AsyncIterator:
    ...
@overload
def run_gen_step_iter[**Args](
    gen_step: Generator | Callable[Args, Generator], 
    async_: Literal[False] = False, 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
) -> Iterator:
    ...
@overload
def run_gen_step_iter[**Args](
    gen_step: Generator | Callable[Args, Generator], 
    async_: Literal[True], 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
) -> AsyncIterator:
    ...
def run_gen_step_iter[**Args](
    gen_step: Generator | Callable[Args, Generator], 
    async_: None | Literal[False, True] = None, 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
) -> Iterator | AsyncIterator:
    """驱动生成器运行，并从中返回可迭代而出的值
    """
    if async_ is None:
        async_ = _get_async()
    if not isinstance(gen_step, Generator):
        params = signature(gen_step).parameters
        if ((param := params.get("async_")) and 
            (param.kind in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY))
        ):
            kwds.setdefault("async_", async_)
        gen_step = gen_step(*args, **kwds)
    if async_:
        return _run_gen_step_async_iter(gen_step)
    else:
        return _run_gen_step_iter(gen_step)


def as_gen_step[**Args](
    gen_step: Callable[Args, Generator], 
    /, 
) -> Callable[Args, Any]:
    default_async = _get_async(default=None)
    def wrapper(*args: Args.args, **kwds: Args.kwargs):
        return run_gen_step(
            gen_step, 
            _coalesce((
                kwds.pop("async_", None), 
                _get_async(default=default_async), 
            )), 
            *args, 
            **kwds, 
        )
    return wrapper


def as_gen_step_iter[**Args](
    gen_step: Callable[Args, Generator], 
    /, 
) -> Callable[Args, Iterable | AsyncIterable]:
    default_async = _get_async(default=None)
    def wrapper(*args: Args.args, **kwds: Args.kwargs):
        return run_gen_step_iter(
            gen_step, 
            _coalesce((
                kwds.pop("async_", None), 
                _get_async(default=default_async), 
            )), 
            *args, 
            **kwds, 
        )
    return wrapper


@overload
def split_cm[T](
    cm: AbstractContextManager[T], 
    /, 
) -> tuple[Callable[[], T], Callable[[], Any]]:
    ...
@overload
def split_cm[T](
    cm: AbstractAsyncContextManager[T], 
    /, 
) -> tuple[Callable[[], Coroutine[Any, Any, T]], Callable[[], Coroutine]]:
    ...
def split_cm[T](
    cm: AbstractContextManager[T] | AbstractAsyncContextManager[T], 
    /, 
) -> (
    tuple[Callable[[], T], Callable[[], Any]] | 
    tuple[Callable[[], Coroutine[Any, Any, T]], Callable[[], Coroutine]]
):
    """拆分上下文管理器，以供 `run_gen_step` 和 `run_gen_step_iter` 使用

    .. code:: python

        if async_:
            async def process():
                async with cm as obj:
                    do_what_you_want()
            return process()
        else:
            with cm as obj:
                do_what_you_want()

    大概相当于

    .. code:: python

        def gen_step():
            enter, exit = split_cm(cm)
            obj = yield enter()
            try:
                do_what_you_want()
            finally:
                yield exit()

        run_gen_step(gen_step, async_)
    """
    if isinstance(cm, AbstractAsyncContextManager):
        enter: Callable = cm.__aenter__
        exit: Callable  = cm.__aexit__
    else:
        enter = cm.__enter__
        exit  = cm.__exit__
    return enter, lambda: exit(*exc_info())


@overload
def with_iter_next[T](
    iterable: Iterable[T], 
    /, 
) -> ContextManager[Callable[[], T]]:
    ...
@overload
def with_iter_next[T](
    iterable: AsyncIterable[T], 
    /, 
) -> ContextManager[Callable[[], Awaitable[T]]]:
    ...
@contextmanager
def with_iter_next[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
):
    """包装迭代器，以供 `run_gen_step` 和 `run_gen_step_iter` 使用

    .. code:: python

        if async_:
            async def process():
                async for e in iterable:
                    do_what_you_want()
            return process()
        else:
            for e in iterable:
                do_what_you_want()

    大概相当于

    .. code:: python

        def gen_step():
            with with_iter_next(iterable) as do_next:
                while True:
                    e = yield do_next()
                    do_what_you_want()

        run_gen_step(gen_step, async_)
    """
    if isinstance(iterable, AsyncIterable):
        try:
            yield aiter(iterable).__anext__
        except StopAsyncIteration:
            pass
    else:
        try:
            yield iter(iterable).__next__
        except StopIteration:
            pass


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
    if async_ or not isinstance(iterables, Iterable):
        if isinstance(iterables, Iterable):
            return async_chain.from_iterable(iterables, threaded=threaded)
        else:
            return async_chain.from_iterable(iterables, threaded=threaded)
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
    value: Callable, 
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
        if iterables:
            for args in _zip(iterable, *iterables):
                value(*args)
        else:
            for arg in iterable:
                value(arg)
    else:
        return async_foreach(value, iterable, *iterables, threaded=threaded)


async def async_foreach(
    value: Callable, 
    iterable: Iterable | AsyncIterable, 
    /, 
    *iterables: Iterable | AsyncIterable, 
    threaded: bool = False, 
):
    """
    """
    value = ensure_async(value, threaded=threaded)
    if iterables:
        async for args in async_zip(iterable, *iterables, threaded=threaded):
            await value(*args)
    else:
        async for arg in ensure_aiter(iterable, threaded=threaded):
            await value(arg)


def through(
    iterable: Iterable | AsyncIterable, 
    /, 
    take_while: None | Callable = None, 
    threaded: bool = False, 
):
    """
    """
    if threaded or not isinstance(iterable, Iterable):
        return async_through(iterable, take_while, threaded=threaded)
    elif take_while is None:
        for _ in iterable:
            pass
    else:
        for v in _map(take_while, iterable):
            if not v:
                break


async def async_through(
    iterable: Iterable | AsyncIterable, 
    /, 
    take_while: None | Callable = None, 
    threaded: bool = False, 
):
    """
    """
    iterable = ensure_aiter(iterable, threaded=threaded)
    if take_while is None:
        async for _ in iterable:
            pass
    elif take_while is bool:
        async for v in iterable:
            if not v:
                break
    else:
        async for v in async_map(take_while, iterable):
            if not v:
                break


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

