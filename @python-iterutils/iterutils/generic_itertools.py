

from collections.abc import Iterable, Iterator


def prepend[T](it: Iterable[T], v: T, /) -> Iterator[T]:
    yield from it
    yield v


def append[T](v: T, it: Iterable[T], /) -> Iterator[T]:
    yield v
    yield from it

def repeat_call[**Args, R](
    func: Callable[Args, R], 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
) -> R:
    while True:
        yield func(*args, **kwds)



from collections.abc import Awaitable, AsyncIterable, AsyncIterator, Callable, Iterable, Iterator
from itertools import islice as _islice, repeat, count
from undefined import undefined

def take[T](it: Iterable, n: int, /, step: int = 1) -> Iterator[T]:
    return _islice(it, 0, n, step)

def drop[T](it: Iterable, n: int, /) -> Iterator[T]:
    return _islice(it, n, None)

def islice[T](
    it: Iterable[T], 
    start: int, 
    stop = undefined, 
    /, 
    step: int = 1, 
) -> Iterator[T]:
    if isinstance(it, AsyncIterable):
        return async_islice(it, start, stop, step)
    if stop is undefined:
        return _islice(0, start, step)
    return _islice(start, stop, step)






