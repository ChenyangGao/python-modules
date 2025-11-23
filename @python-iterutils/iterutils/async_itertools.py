
from collections.abc import AsyncIterable, AsyncIterator


async def async_count(start: int = 0, step: int = 1, /) -> AsyncIterator[int]:
    while True:
        yield start
        start += step

async def async_enumerate[T](it: AsyncIterable[T], start: int = 0, /) -> tuple[int, T]:
    i = start
    async for e in it:
        yield i, t
        i += 1

async def async_repeat_call[**Args, R](
    func: Callable[Args, Awaitable[R]], 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
) -> R:
    while True:
        ret = func(*args, **kwds)
        if isawaitable(ret):
            ret = await ret
        yield ret


async def async_repeat[T](e: T, n: None | int = None, /) -> AsyncIterator[T]:
    if n is None:
        while True:
            yield e
    elif n > 0:
        for _ in repeat(None, n):
            yield e



async def async_drop[T](
    it: AsyncIterable[T], 
    n: int = 1, 
    /, 
) -> AsyncIterator[T]:
    assert n >= 0
    it = aiter(it)
    try:
        getnext = it.__anext__
        for _ in repeat(None, n):
            await getnext()
    except StopAsyncIteration:
        return
    async for e in it:
        yield e

async def async_take[T](
    it: AsyncIterable[T], 
    n: int = 1, 
    /, 
    step: int = 1, 
) -> AsyncIterator[T]:
    assert step >= 1
    getnext = aiter(it).__anext__
    try:
        if step == 1:
            for _ in repeat(None, n):
                yield (await getnext())
        else:
            interval = step - 1
            for _ in range(n // step):
                yield (await getnext())
                for _ in repeat(None, interval):
                    await getnext()
            yield (await getnext())
    except StopAsyncIteration:
        return

def async_islice(
    it: AsyncIterable, 
    start: int, 
    stop = undefined, 
    /, 
    step: int = 1, 
):
    if stop is undefined:
        return async_take(it, start, step)
    else:
        return async_take(async_drop(it, start), stop - start, step)



