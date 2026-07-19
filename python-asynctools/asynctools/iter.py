#!/usr/bin/env python3
# encoding: utf-8

__all__ = [
    "async_range", "async_enumerate", "async_zip", "async_map", "async_filter", 
    "async_accumulate", "async_batched", "async_chain", "async_chain_from_iterable", 
    "async_compress", "async_count", "async_cycle", "async_dropwhile", "async_filterfalse", 
    "async_groupby", "async_islice", "async_pairwise", "async_repeat", "async_starmap", 
    "async_takewhile", "async_tee", "async_zip_longest", 
    "async_product", "async_permutations", "async_combinations", "async_combinations_with_replacement", 
]

from asyncio import create_task, wait, Task
from collections.abc import (
    Awaitable, Callable, Iterable, AsyncIterable, AsyncIterator, MutableSequence, Sequence, 
)
from itertools import (
    batched, count, cycle, repeat, pairwise, product, permutations, combinations, 
    combinations_with_replacement, 
)
from typing import cast, overload, Any, Self

from undefined import undefined, Undefined

from .basic import ensure_aiter, ensure_async, ensure_coroutine, to_aiter
from .reduce import async_collect
from .type import SupportsBool


def async_range(
    start: int = 0, 
    stop: None | int = None, 
    /, 
    step: int = 1, 
) -> AsyncIterator[int]:
    if stop is None:
        start, stop = 0, start
    return to_aiter(range(start, stop, step))


def async_enumerate[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    start: int = 0, 
    threaded: bool = False, 
) -> AsyncIterator[tuple[int, T]]:
    if threaded or isinstance(iterable, AsyncIterable):
        return async_zip(async_count(start), ensure_aiter(iterable, threaded=threaded))
    else:
        return to_aiter(enumerate(iterable, start))


async def _async_zip(
    *iterables: Iterable | AsyncIterable, 
    strict: bool = False, 
    threaded: bool = False, 
) -> AsyncIterator[tuple]:
    ls_anext = [ensure_aiter(it, threaded=threaded).__anext__ for it in iterables]
    while True:
        try:
            tasks = [create_task(ensure_coroutine(anext())) for anext in ls_anext]
            await wait(tasks)
            if strict:
                exhausted = sum(isinstance(t.exception(), StopAsyncIteration) for t in tasks)
                if exhausted:
                    count = len(iterables)
                    if exhausted == count:
                        return
                    raise ValueError(f"{exhausted} of {count} tasks were already exhausted")
            yield tuple(t.result() for t in tasks)
        except StopAsyncIteration:
            break


def async_zip(
    iterable: Iterable | AsyncIterable, 
    /, 
    *iterables: Iterable | AsyncIterable, 
    strict: bool = False, 
    threaded: bool = False, 
) -> AsyncIterator[tuple]:
    if iterables:
        return _async_zip(iterable, *iterables, strict=strict, threaded=threaded)
    return ((e,) async for e in ensure_aiter(iterable, threaded=threaded))


async def async_map[T](
    function: Callable[..., Awaitable[T]] | Callable[..., T], 
    iterable: Iterable | AsyncIterable, 
    /, 
    *iterables: Iterable | AsyncIterable, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    function = ensure_async(function, threaded=threaded)
    if iterables:
        async for args in async_zip(iterable, *iterables, threaded=threaded):
            yield await function(*args)
    else:
        async for arg in ensure_aiter(iterable, threaded=threaded):
            yield await function(arg)


async def async_filter[T](
    function: None | Callable[[T], Awaitable[SupportsBool]] | Callable[[T], SupportsBool], 
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    if function is None:
        async for arg in ensure_aiter(iterable, threaded=threaded):
            if arg:
                yield arg
    else:
        function = ensure_async(function, threaded=threaded)
        async for arg in ensure_aiter(iterable, threaded=threaded):
            if await function(arg):
                yield arg


async def async_accumulate[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    function: Callable[[T, T], Awaitable[T]] | Callable[[T, T], T], 
    /, 
    initial: Undefined | T = undefined, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    iterator = ensure_aiter(iterable, threaded=threaded)
    if initial is undefined:
        try:
            initial = await anext(iterator)
        except StopAsyncIteration:
            return
    initial = cast(T, initial)
    yield initial
    call = ensure_async(function, threaded=threaded)
    async for e in iterator:
        initial = await call(initial, e)
        yield initial


def async_batched[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    n: int = 1, 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[tuple[T, ...]]:
    if n < 1:
        raise ValueError(f"`n` must be at least 1, got {n!r}")
    if isinstance(iterable, Sequence):
        if n == 1:
            return to_aiter(zip(iterable))
        else:
            return to_aiter(map(
                lambda t, /: tuple(iterable[slice(*t)]), 
                pairwise(range(0, len(iterable)+n, n)), 
            ))
    elif threaded or isinstance(iterable, AsyncIterable):
        from .misc import async_iter
        async def get(it=ensure_aiter(iterable, threaded=threaded), /):
            return tuple([a async for a in async_islice(it, n)])
        return async_iter(get, ())
    else:
        return to_aiter(batched(iterable, n))


async def async_chain[T](
    *iterables: Iterable[T] | AsyncIterable[T], 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    for iterable in iterables:
        async for e in ensure_aiter(iterable, threaded=threaded):
            yield e


async def async_chain_from_iterable[T](
    iterables: Iterable[Iterable[T] | AsyncIterable[T]] | AsyncIterable[Iterable[T] | AsyncIterable[T]], 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    if isinstance(iterables, Iterable):
        for iterable in iterables:
            async for e in ensure_aiter(iterable, threaded=threaded):
                yield e
    else:
        async for iterable in iterables:
            async for e in ensure_aiter(iterable, threaded=threaded):
                yield e

setattr(async_chain, "from_iterable", async_chain_from_iterable)


def async_compress[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    selectors: Iterable[T] | AsyncIterable[T], 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    return (e async for e, s in async_zip(iterable, selectors, threaded=threaded) if s)


def async_count(
    start: int = 0, 
    step: int = 1, 
) -> AsyncIterator[int]:
    return to_aiter(count(start, step))


async def _async_cycle[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    seq: Sequence
    if isinstance(iterable, Sequence):
        seq = tuple(seq)
        if isinstance(seq, MutableSequence):
            seq = tuple(seq)
    else:
        seq = []
        add = seq.append
        if threaded or isinstance(iterable, AsyncIterable):
            async for e in ensure_aiter(iterable, threaded=threaded):
                yield e
                add(e)
        else:
            for e in iterable:
                yield e
                add(e)
    if seq:
        while True:
            for e in seq:
                yield e


def async_cycle[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    if threaded or isinstance(iterable, AsyncIterable):
        return _async_cycle(iterable, threaded=threaded)
    else:
        return to_aiter(cycle(iterable))


async def async_dropwhile[T](
    predicate: Callable[[T], Awaitable[SupportsBool]] | Callable[[T], SupportsBool], 
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    predicate = ensure_async(predicate, threaded=threaded)
    iterator = ensure_aiter(iterable, threaded=threaded)
    async for e in iterator:
        if not await predicate(e):
            yield e
            break
    async for e in iterator:
        yield e


async def async_filterfalse[T](
    predicate: None | Callable[[T], Awaitable[SupportsBool]] | Callable[[T], SupportsBool], 
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    if predicate is None:
        async for e in ensure_aiter(iterable, threaded=threaded):
            if not e:
                yield e
    else:
        predicate = ensure_async(predicate, threaded=threaded)
        async for e in ensure_aiter(iterable, threaded=threaded):
            if not await predicate(e):
                yield e


@overload
def async_groupby[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    key: None = None, 
    threaded: bool = False, 
) -> AsyncIterator[tuple[T, AsyncIterator[T]]]:
    ...
@overload
def async_groupby[T, K](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    key: Callable[[T], K], 
    threaded: bool = False, 
) -> AsyncIterator[tuple[K, AsyncIterator[T]]]:
    ...
async def async_groupby[T, K](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    key: None | Callable[[T], K] = None, 
    threaded: bool = False, 
) -> AsyncIterator[tuple[T, AsyncIterator[T]]] | AsyncIterator[tuple[K, AsyncIterator[T]]]:
    iterator = ensure_aiter(iterable, threaded=threaded)
    try:
        cur_val = await anext(iterator)
    except StopAsyncIteration:
        return
    cur_key: Any = cur_val if key is None else key(cur_val)
    exhausted = False

    async def grouper(target_key, /) -> AsyncIterator[T]:
        nonlocal cur_key, cur_val, exhausted
        yield cur_val
        async for cur_val in iterator:
            cur_key = cur_val if key is None else key(cur_val)
            if not (cur_key is target_key or cur_key == target_key):
                return
            yield cur_val
        exhausted = True

    while not exhausted:
        target_key = cur_key
        cur_group = grouper(cur_key)
        yield cur_key, cur_group
        if cur_key == target_key:
            async for _ in cur_group:
                pass


async def async_islice[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    start: int = 0, 
    stop: None | int = None, 
    /, 
    step: int = 1, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    if stop is None:
        start, stop = 0, start
    enum = async_enumerate(iterable, threaded=threaded)
    for idx in range(start, stop, step):
        async for i, e in enum:
            if i == idx:
                yield e
                break


async def async_pairwise[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[tuple[T, T]]:
    iterator = ensure_aiter(iterable, threaded=threaded)
    try:
        l = await anext(iterator)
    except StopAsyncIteration:
        return
    async for r in iterator:
        yield l, r
        l = r


def async_repeat[T](value: T, /, times: None | int = -1) -> AsyncIterator[T]:
    if times is None or times < 0:
        return to_aiter(repeat(value))
    return to_aiter(repeat(value, times))


async def async_starmap[T](
    function: Callable[..., Awaitable[T]] | Callable[..., T], 
    iterable: Iterable | AsyncIterable, 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    function = ensure_async(function, threaded=threaded)
    async for args in ensure_aiter(iterable, threaded=threaded):
        yield await function(*args)


async def async_takewhile[T](
    predicate: Callable[[T], Awaitable[SupportsBool]] | Callable[[T], SupportsBool], 
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    predicate = ensure_async(predicate, threaded=threaded)
    async for e in ensure_aiter(iterable, threaded=threaded):
        if not await predicate(e):
            break
        yield e


class _tee[T](AsyncIterator[T]):

    def __init__(self, iterator: AsyncIterator[T], /):
        if isinstance(iterator, _tee):
            self.iterator: AsyncIterator[T] = iterator.iterator
            self.link: list = iterator.link
        else:
            self.iterator = iterator
            self.link = [None, None]

    def __aiter__(self, /) -> Self:
        return self

    async def __anext__(self, /) -> T:
        link = self.link
        if link[1] is None:
            link[0] = await anext(self.iterator)
            link[1] = [None, None]
        value, self.link = link
        return value


def async_tee[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    n: int = 2, 
    /, 
    threaded: bool = False, 
) -> tuple[AsyncIterator[T], ...]:
    if n < 0:
        raise ValueError(f"n must be >= 0, got {n}")
    if n == 0:
        return ()
    iterator = _tee(ensure_aiter(iterable, threaded=threaded))
    if n == 1:
        return iterator,
    iterators = [iterator]
    iterators.extend(_tee(iterator) for _ in repeat(None, n-1))
    return tuple(iterators)


async def _async_zip_longest(
    *iterables: Iterable | AsyncIterable, 
    fillvalue = None, 
    threaded: bool = False, 
) -> AsyncIterator[tuple]:
    num_active = len(iterables)
    ls_get_next: list[Any] = [ensure_aiter(it, threaded=threaded).__anext__ for it in iterables]
    values: list = []
    add = values.append
    clear = values.clear
    while True:
        for i, get_next in enumerate(ls_get_next):
            value: Any = fillvalue
            if get_next is not None:
                try:
                    value = await get_next()
                except StopAsyncIteration:
                    num_active -= 1
                    if not num_active:
                        return
                    ls_get_next[i] = None
            add(value)
        yield tuple(values)
        clear()


def async_zip_longest(
    iterable: Iterable | AsyncIterable, 
    /, 
    *iterables: Iterable | AsyncIterable, 
    fillvalue = None, 
    threaded: bool = False, 
) -> AsyncIterator[tuple]:
    if not iterables:
        return async_zip(iterable, threaded=threaded)
    return _async_zip_longest(
        iterable, 
        *iterables, 
        fillvalue=fillvalue, 
        threaded=threaded, 
    )


async def async_product(
    *iterables: Iterable | AsyncIterable, 
    repeat: int = 1, 
    threaded: bool = False, 
) -> AsyncIterator[tuple]:
    if repeat < 0:
        raise ValueError("repeat must be >= 0")
    elif repeat: 
        for e in product(*[await async_collect(it, threaded=threaded) for it in iterables], repeat=repeat):
            yield e


async def async_permutations[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    r: None | int = None, 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[tuple[T, ...]]:
    if r and r < 0:
        raise ValueError("r must be >= 0")
    elif r:
        pool = await async_collect(iterable, threaded=threaded)
        for e in permutations(pool, r):
            yield e


async def async_combinations[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    r: int, 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[tuple[T, ...]]:
    if r < 0:
        raise ValueError("r must be >= 0")
    elif r:
        pool = await async_collect(iterable, threaded=threaded)
        for e in combinations(pool, r):
            yield e


async def async_combinations_with_replacement[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    r: int, 
    /, 
    threaded: bool = False, 
) -> AsyncIterator[tuple[T, ...]]:
    if r < 0:
        raise ValueError("r must be >= 0")
    elif r:
        pool = await async_collect(iterable, threaded=threaded)
        for e in combinations_with_replacement(pool, r):
            yield e

