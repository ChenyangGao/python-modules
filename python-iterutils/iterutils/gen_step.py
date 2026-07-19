#!/usr/bin/env python3
# encoding: utf-8

__all__ = [
    "iter_gen_step", "run_gen_step", "run_gen_step_iter", 
    "as_gen_step", "with_iter_next", "with_lock", "split_cm", 
    "Yield", "YieldFrom", 
]

from asyncio import CancelledError as AsyncCancelledError
from collections.abc import (
    AsyncGenerator, AsyncIterable, AsyncIterator, Awaitable, Callable, 
    Coroutine, Generator, Iterable, Iterator, 
)
from concurrent.futures import CancelledError
from contextlib import contextmanager, AbstractContextManager, AbstractAsyncContextManager
from dataclasses import dataclass
from inspect import isawaitable
from sys import exc_info
from typing import (
   cast,  overload, runtime_checkable, Any, ContextManager, Literal, Protocol, 
)

from argtools import has_keyword_arg
from asynctools import in_async
from decotools import optional


@runtime_checkable
class SupportsBool(Protocol):
    def __bool__(self, /) -> bool:
        ...


@dataclass(slots=True, frozen=True)
class Yield:
    """专供 `run_gen_step_iter`，说明值需要 yield 给用户
    """
    value: Any


@dataclass(slots=True, frozen=True)
class YieldFrom:
    """专供 `run_gen_step_iter`，说明值需要解包后逐个 yield 给用户
    """
    value: Any


def iter_gen_step_sync[Y](
    gen: Generator[Y, None | Y, Y], 
    /, 
) -> Generator[Y, Any, Any]:
    send = gen.send
    try:
        v = None
        while True:
            yield (v := send(v))
    except StopIteration as e:
        yield e.value
    finally:
        gen.close()


async def iter_gen_step_async[Y](
    gen: Generator[Awaitable[Y], None | Y, Awaitable[Y] | Y], 
    /, 
) -> AsyncGenerator[Y, Any]:
    send  = gen.send
    throw = gen.throw
    try:
        r = send(None)
        while True:
            try:
                yield (v := await r)
            except BaseException as e:
                r = throw(e)
            else:
                r = send(v)
        raise AsyncCancelledError
    except StopIteration as e:
        v = e.value
        if isawaitable(v):
            v = await v
        yield v
    finally:
        gen.close()


@overload
def iter_gen_step[Y](
    gen: Generator[Y, None | Y, Y] | Callable[[], Generator[Y, None | Y, Y]], 
    /, 
    async_: Literal[False] = False, 
) -> Generator[Y, Any, Any]:
    ...
@overload
def iter_gen_step[Y](
    gen: Generator[Awaitable[Y], None | Y, Awaitable[Y] | Y] | Callable[[], Generator[Awaitable[Y], None | Y, Awaitable[Y] | Y]], 
    /, 
    async_: Literal[True], 
) -> AsyncGenerator[Y, Any]:
    ...
def iter_gen_step[Y](
    gen: Generator[Y, None | Y, Y] | Callable[[], Generator[Y, None | Y, Y]] | Generator[Awaitable[Y], None | Y, Awaitable[Y] | Y] | Callable[[], Generator[Awaitable[Y], None | Y, Awaitable[Y] | Y]], 
    /, 
    async_: Literal[False, True] = False, 
) -> Generator[Y, Any, Any] | AsyncGenerator[Y, Any]:
    if not isinstance(gen, Generator):
        gen = gen()
    if async_:
        gen = cast(Generator[Awaitable[Y], None | Y, Awaitable[Y] | Y], gen)
        return iter_gen_step_async(gen)
    else:
        gen = cast(Generator[Y, None | Y, Y], gen)
        return iter_gen_step_sync(gen)


def run_gen_step_sync[Y, T](
    gen: Generator[Y, None | Y, T], 
    /, 
    running: SupportsBool = True, 
) -> T:
    send = gen.send
    try:
        v = None
        while running:
            v = send(v)
        raise CancelledError
    except StopIteration as e:
        return e.value
    finally:
        gen.close()


async def run_gen_step_async[Y, T](
    gen: Generator[Awaitable[Y], None | Y, Awaitable[T] | T], 
    /, 
    running: SupportsBool = True, 
) -> T:
    send  = gen.send
    throw = gen.throw
    try:
        r = send(None)
        while running:
            try:
                v = await r
            except BaseException as e:
                r = throw(e)
            else:
                r = send(v)
        raise AsyncCancelledError
    except StopIteration as e:
        v = e.value
        if isawaitable(v):
            v = await v
        return v
    finally:
        gen.close()


@overload
def run_gen_step[T](
    gen: Generator[Any, Any, T] | Callable[[], Generator[Any, Any, T]], 
    /, 
    async_: Literal[False] = False, 
    running: SupportsBool = True, 
) -> T:
    ...
@overload
def run_gen_step[T](
    gen: Generator[Awaitable, Any, Awaitable[T] | T] | Callable[[], Generator[Awaitable, Any, Awaitable[T] | T]], 
    /, 
    async_: Literal[True], 
    running: SupportsBool = True, 
) -> Coroutine[Any, Any, T]:
    ...
def run_gen_step(
    gen, 
    /, 
    async_: Literal[False, True] = False, 
    running: SupportsBool = True, 
):
    if not isinstance(gen, Generator):
        gen = gen()
    if async_:
        return run_gen_step_async(gen, running)
    else:
        return run_gen_step_sync(gen, running)


def run_gen_step_iter_sync(
    gen: Generator, 
    /, 
    running: SupportsBool = True, 
) -> Iterator:
    send = gen.send
    try:
        v: Any = None
        while running:
            v = send(v)
            if isinstance(v, Yield):
                v = v.value
                yield v
            elif isinstance(v, YieldFrom):
                v = yield from v.value
        raise CancelledError
    except StopIteration as e:
        return e.value
    finally:
        gen.close()


async def run_gen_step_iter_async(
    gen: Generator, 
    /, 
    running: SupportsBool = True, 
) -> AsyncIterator:
    send  = gen.send
    throw = gen.throw
    try:
        v = send(None)
        while running:
            try:
                if isinstance(v, Yield):
                    yield_type = 1
                    v = v.value
                elif isinstance(v, YieldFrom):
                    yield_type = 2
                    v = v.value
                else:
                    yield_type = 0
                if isawaitable(v):
                    v = await v
                match yield_type:
                    case 1:
                        yield v
                    case 2:
                        if isinstance(v, AsyncIterable):
                            async for e in v:
                                yield e
                        else:
                            for e in v:
                                yield e
            except BaseException as e:
                v = throw(e)
            else:
                v = send(v)
    except StopIteration as e:
        v = e.value
        if isawaitable(v):
            v = await v
        #return v
    finally:
        gen.close()


@overload
def run_gen_step_iter(
    gen: Generator | Callable[[], Generator], 
    /, 
    async_: Literal[False] = False, 
    running: SupportsBool = True, 
) -> Iterator:
    ...
@overload
def run_gen_step_iter(
    gen: Generator | Callable[[], Generator], 
    /, 
    async_: Literal[True], 
    running: SupportsBool = True, 
) -> AsyncIterator:
    ...
def run_gen_step_iter(
    gen: Generator | Callable[[], Generator], 
    /, 
    async_: Literal[False, True] = False, 
    running: SupportsBool = True, 
) -> Iterator | AsyncIterator:
    if not isinstance(gen, Generator):
        gen = gen()
    if async_:
        return run_gen_step_iter_async(gen, running)
    else:
        return run_gen_step_iter_sync(gen, running)


@optional
def as_gen_step(
    func: Callable[..., Generator], 
    /, 
    async_: None | bool = None, 
    is_iter: bool = False, 
):
    has_async_arg = has_keyword_arg(func, "async_")
    if is_iter:
        run: Callable = run_gen_step_iter
    else:
        run = run_gen_step
    def wrapper(*args, async_: None | bool = async_, **kwds):
        if async_ is None:
            async_ = in_async()
        if has_async_arg:
            kwds["async_"] = async_
        return run(func(*args, **kwds), async_)
    return wrapper


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
                    do_what_you_want(e)
            return process()
        else:
            for e in iterable:
                do_what_you_want(e)

    大概相当于

    .. code:: python

        def gen_step():
            with with_iter_next(iterable) as do_next:
                while True:
                    e = yield do_next()
                    do_what_you_want(e)

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


@contextmanager
def with_lock[T](lock, /):
    """包装锁，以供 `run_gen_step` 和 `run_gen_step_iter` 使用

    .. code:: python

        if async_:
            async def process():
                async with lock:
                    ...
            return process()
        else:
            with lock:
                ...

    大概相当于

    .. code:: python

        def gen_step():
            with with_lock(lock) as r:
                yield r
                ...

        run_gen_step(gen_step, async_)
    """
    try:
        yield lock.acquire()
    finally:
        lock.release()


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
                    do_what_you_want(obj)
            return process()
        else:
            with cm as obj:
                do_what_you_want(obj)

    大概相当于

    .. code:: python

        def gen_step():
            enter, exit = split_cm(cm)
            obj = yield enter()
            try:
                do_what_you_want(obj)
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

