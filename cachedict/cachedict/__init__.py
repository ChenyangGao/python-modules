#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 4)
__all__ = [
    "SizedDict", "FILODict", "FIFODict", "RRDict", "LRUDict", "LFUDict", 
    "TTLDict", "PriorityDict", "TLRUDict", "FastFIFODict", "FastLRUDict", 
]

from collections.abc import Callable, Hashable
from dataclasses import dataclass, field
from functools import update_wrapper
from heapq import heappush, heappop, nlargest, nsmallest
from inspect import signature, _empty
from itertools import count
from math import inf, isinf, isnan
from operator import itemgetter
from random import randrange
from time import time
from typing import cast, overload, Any, Literal
from warnings import warn

from undefined import undefined, Undefined


class CleanedKeyError(KeyError):

    def __init__(self, key, value, /):
        super().__init__(key, value)
        self.key = key
        self.value = value


class SizedDict[K: Hashable, V](dict[K, V]):
    __slots__ = ("maxsize", "auto_clean")

    def __init__(
        self, 
        /, 
        maxsize: int = 0, 
        auto_clean: bool = True, 
    ):
        self.maxsize = maxsize
        self.auto_clean = auto_clean

    @overload
    def __call__[**P](
        self, 
        func: None = None, 
        /, 
        key: None | Callable[P, K] = None, 
    ) -> Callable[[Callable[P, V]], Callable[P, V]]:
        ...
    @overload
    def __call__[**P](
        self, 
        func: Callable[P, V], 
        /, 
        key: None | Callable[P, K] = None, 
    ) -> Callable[P, V]:
        ...
    def __call__[**P](
        self, 
        func: None | Callable[P, V] = None, 
        /, 
        key: None | Callable[P, K] = None, 
    ) -> Callable[P, V] | Callable[[Callable[P, V]], Callable[P, V]]:
        if func is None:
            def decorator(func: Callable[P, V], /):
                return self(func, key=key)
            return decorator
        if key is None:
            try:
                sig = signature(func)
                params = tuple(sig.parameters.values())
            except ValueError:
                pass
            else:
                if not params:
                    def key(): # type: ignore
                        return None
                else:
                    param = params[0]
                    if len(params) == 1 and param.kind is param.POSITIONAL_ONLY and param.default is _empty:
                        def key(arg, /): # type: ignore
                            return arg
                    elif all(p.kind in (param.POSITIONAL_ONLY, param.VAR_POSITIONAL) for p in params):
                        def key(*args): # type: ignore
                            return args
                    elif all(p.kind in (param.KEYWORD_ONLY, param.VAR_KEYWORD) for p in params):
                        def key(**kwds): # type: ignore
                            return tuple(kwds.items())
                    elif params[-1].kind is param.VAR_KEYWORD:
                        kwds_name = params[-1].name
                        def key(*args: P.args, **kwds: P.kwargs):
                            bound_args = sig.bind(*args, **kwds)
                            arguments = bound_args.arguments
                            try:
                                kwargs = arguments.pop(kwds_name)
                            except KeyError:
                                return tuple(arguments.items()), ()
                            return tuple(arguments.items()), tuple(kwargs.items())
                    else:
                        def key(*args: P.args, **kwds: P.kwargs):
                            bound_args = sig.bind(*args, **kwds)
                            return tuple(bound_args.arguments.items())
            if key is None:
                def key(*args: P.args, **kwds: P.kwargs):
                    return args, tuple(kwds.items())
        def wrapper(*args: P.args, **kwds: P.kwargs) -> V:
            try:
                k = key(*args, **kwds)
            except Exception as e:
                args_str = ", ".join((
                    ", ".join(map(repr, args)), 
                    ", ".join(f"{k}={v!r}" for k, v in kwds.items()), 
                ))
                exctype = type(e)
                if exctype.__module__ in ("builtins", "__main__"):
                    exc_name = exctype.__qualname__
                else:
                    exc_name = f"{exctype.__module__}.{exctype.__qualname__}"
                warn(f"{key!r}({args_str}) encountered an error {exc_name}: {e}")
                return func(*args, **kwds)
            try:
                return self[k]
            except KeyError:
                v = self[k] = func(*args, **kwds)
                return v
            except TypeError:
                return func(*args, **kwds)
        return update_wrapper(wrapper, func)

    def __repr__(self, /) -> str:
        if self.auto_clean:
            self.clean()
        cls = type(self)
        return f"<{cls.__module__}.{cls.__qualname__} object at {hex(id(self))} with {super().__repr__()}>"

    def __contains__(self, key, /) -> bool:
        try:
            self[key]
            return True
        except (KeyError, TypeError):
            return False

    def __setitem__(self, key: K, value: V, /):
        if key not in self and self.auto_clean:
            self.clean(1)
        super().__setitem__(key, value)

    def clean(self, /, extra: int = 0) -> list[tuple[K, V]]:
        items: list[tuple[K, V]] = []
        add_item = items.append
        if self and (maxsize := self.maxsize) > 0:
            remains = maxsize - extra
            if remains <= 0:
                self.clear()
            else:
                popitem = self.popitem
                try:
                    while len(self) > remains:
                        add_item(popitem())
                except KeyError:
                    pass
        return items

    def discard(self, key, /):
        try:
            del self[key]
        except (KeyError, TypeError):
            pass

    def clear(self, /):
        discard = self.discard
        try:
            while True:
                discard(next(iter(self)))
        except StopIteration:
            pass

    @overload
    def get(self, key: K, /, default: None = None) -> None | V:
        ...
    @overload
    def get[T](self, key: K, /, default: T) -> V | T:
        ...
    def get[T](self, key: K, /, default: None | V | T = None) -> None | V | T:
        try:
            return self[key]
        except KeyError:
            return default

    @overload
    def pop(self, key: K, /, default: Undefined = undefined) -> V:
        ...
    @overload
    def pop(self, key: K, /, default: V) -> V:
        ...
    @overload
    def pop[T](self, key: K, /, default: T) -> V | T:
        ...
    def pop[T](self, key: K, /, default: Undefined | V | T = undefined) -> V | T:
        try:
            val = self[key]
            self.discard(key)
            return val
        except KeyError:
            if default is undefined:
                raise
            return cast(V | T, default)

    def popitem(self, /) -> tuple[K, V]:
        try:
            while True:
                try:
                    key = next(iter(reversed(self)))
                    return key, self.pop(key)
                except CleanedKeyError as e:
                    return e.key, e.value
                except (KeyError, RuntimeError):
                    pass
        except StopIteration:
            pass
        raise KeyError(f"{self!r} is empty")

    def setdefault(self, key: K, default: V, /) -> V:
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def update(self, /, *args, **pairs):
        cache: dict = {}
        try:
            update = cache.update
            m: Any
            for m in filter(None, args):
                update(m)
            if pairs:
                update(pairs)
        finally:
            if cache_size := len(cache):
                maxsize = self.maxsize
                if self.auto_clean and maxsize > 0:
                    self.clean(cache_size)
                start = max(0, cache_size - maxsize)
                for i, (k, v) in enumerate(cache.items()):
                    if i >= start:
                        self[k] = v


class FILODict[K: Hashable, V](SizedDict[K, V]):
    __slots__ = ("maxsize", "auto_clean")

    def __setitem__(self, key: K, value: V, /):
        self.discard(key)
        super().__setitem__(key, value)


class FIFODict[K: Hashable, V](FILODict[K, V]):
    __slots__ = ("maxsize", "auto_clean")

    def popitem(self, /) -> tuple[K, V]:
        try:
            while True:
                try:
                    key = next(iter(self))
                    return key, self.pop(key)
                except CleanedKeyError as e:
                    return e.key, e.value
                except (KeyError, RuntimeError):
                    pass
        except StopIteration:
            pass
        raise KeyError(f"{self!r} is empty")


class RRDict[K: Hashable, V](SizedDict[K, V]):
    __slots__ = ("maxsize", "auto_clean")

    def popitem(self, /) -> tuple[K, V]:
        try:
            while True:
                try:
                    idx = randrange(len(self))
                    for i, key in enumerate(self):
                        if i == idx:
                            break
                    return key, self.pop(key)
                except CleanedKeyError as e:
                    return e.key, e.value
                except (KeyError, RuntimeError):
                    pass
        except StopIteration:
            pass
        raise KeyError(f"{self!r} is empty")


class LRUDict[K: Hashable, V](FIFODict[K, V]):
    __slots__ = ("maxsize", "auto_clean")

    def __getitem__(self, key: K, /) -> V:
        value = super().__getitem__(key)
        self.discard(key)
        self[key] = value
        return value


class Counter[K](dict[K, int]):

    def __missing__(self, _: K, /) -> Literal[0]:
        return 0

    def max(self, /) -> tuple[K, int]:
        try:
            return max(self.items(), key=itemgetter(1))
        except ValueError as e:
            raise KeyError(f"{self!r} is empty") from e

    def min(self, /) -> tuple[K, int]:
        try:
            return min(self.items(), key=itemgetter(1))
        except ValueError as e:
            raise KeyError(f"{self!r} is empty") from e

    def most_common(
        self, 
        n: None | int = None, 
        /, 
        largest: bool = True, 
    ) -> list[tuple[K, int]]:
        if n is None:
            return sorted(self.items(), key=itemgetter(1), reverse=largest)
        if largest:
            return nlargest(n, self.items(), key=itemgetter(1))
        else:
            return nsmallest(n, self.items(), key=itemgetter(1))


class LFUDict[K: Hashable, V](SizedDict[K, V]):
    __slots__ = ("maxsize", "auto_clean", "_counter")

    def __init__(
        self, 
        /, 
        maxsize: int = 0, 
        auto_clean: bool = True, 
    ):
        super().__init__(maxsize, auto_clean)
        self._counter: Counter[K] = Counter()

    def __delitem__(self, key: K, /):
        super().__delitem__(key)
        self._counter.pop(key, None)

    def __getitem__(self, key: K, /) -> V:
        value = super().__getitem__(key)
        self._counter[key] += 1
        return value

    def __setitem__(self, key: K, value: V, /):
        super().__setitem__(key, value)
        self._counter[key] += 1


class TTLDict[K, V](SizedDict[K, V]):
    __slots__ = ("maxsize", "auto_clean", "ttl", "timer", "_ttl_cache")

    def __init__(
        self, 
        /, 
        ttl: int | float = inf, 
        timer: Callable[[], int | float] = time, 
        maxsize: int = 0, 
        auto_clean: bool = True, 
    ):
        super().__init__(maxsize, auto_clean)
        self.ttl = ttl
        self.timer = timer
        self._ttl_cache: dict[K, int | float] = {}

    def __delitem__(self, key: K, /):
        super().__delitem__(key)
        self._ttl_cache.pop(key, None)

    def __getitem__(self, key: K, /) -> V:
        value = super().__getitem__(key)
        ttl = self.ttl
        if isinf(ttl) or isnan(ttl) or ttl <= 0:
            return value
        ttl_cache = self._ttl_cache
        start = ttl_cache[key]
        diff = ttl + start - self.timer()
        if diff <= 0:
            self.discard(key)
            ttl_cache.pop(key, None)
            if diff:
                raise CleanedKeyError(key, value)
        return value

    def __setitem__(self, key: K, value: V, /):
        ttl_cache = self._ttl_cache
        super().__setitem__(key, value)
        ttl_cache.pop(key, None)
        ttl_cache[key] = self.timer()
        if self.auto_clean:
            self.clean()

    def clean(self, /, extra: int = 0) -> list[tuple[K, V]]:
        items = super().clean(extra)
        ttl = self.ttl
        if self and not (isinf(ttl) or isnan(ttl) or ttl <= 0):
            add_item = items.append
            pop   = self.pop
            thres = self.timer() - ttl
            ttl_items = self._ttl_cache.items()
            while True:
                try:
                    key, ts = next(iter(ttl_items))
                    if ts > thres:
                        break
                    add_item((key, pop(key)))
                except CleanedKeyError as e:
                    add_item((e.key, e.value))
                except (KeyError, RuntimeError):
                    pass
                except StopIteration:
                    break
        return items


@dataclass(slots=True, order=True)
class KeyPriority[F, K]:
    priority: F
    number: int = field(default_factory=count(1).__next__)
    key: K | Undefined = undefined


class PriorityDict[K: Hashable, V](SizedDict[K, V]):
    __slots__ = ("maxsize", "auto_clean", "priority", "watermark", "_heap", "_key_to_entry")

    def __init__(
        self, 
        /, 
        priority: Callable[[V], Any] = lambda _: 0, 
        watermark: None | Callable[[], Any] = None, 
        maxsize: int = 0, 
        auto_clean: bool = True, 
    ):
        super().__init__(maxsize, auto_clean)
        self.priority = priority
        self.watermark = watermark
        self._heap: list[KeyPriority] = []
        self._key_to_entry: dict[K, KeyPriority] = {}

    def __delitem__(self, key: K, /):
        super().__delitem__(key)
        if entry := self._key_to_entry.pop(key, None):
            entry.key = undefined

    def __getitem__(self, key: K) -> V:
        value = super().__getitem__(key)
        if watermark := self.watermark:
            priority = self.priority(value)
            if watermark() >= priority:
                self.discard(key)
                raise CleanedKeyError(key, value)
        return value

    def __setitem__(self, key: K, value: V, /):
        self.discard(key)
        super().__setitem__(key, value)
        entry = self._key_to_entry[key] = KeyPriority(self.priority(value), key=key)
        heappush(self._heap, entry)

    def popitem(self, /) -> tuple[K, V]:
        try:
            heap = self._heap
            while heap:
                try:
                    key = heappop(heap).key
                    if key is undefined:
                        continue
                    key = cast(K, key)
                    return key, self.pop(key)
                except CleanedKeyError as e:
                    return e.key, e.value
                except (LookupError, RuntimeError):
                    pass
        except StopIteration:
            pass
        raise KeyError(f"{self!r} is empty")

    def clean(self, /, extra: int = 0) -> list[tuple[K, V]]:
        items = super().clean()
        add_item = items.append
        if watermark := self.watermark:
            heap = self._heap
            popitem = self.popitem
            watermark_value = watermark()
            try:
                while True:
                    entry = heap[0]
                    key = entry.key
                    if key is not undefined and watermark_value < entry.priority:
                        break
                    entry1 = heappop(heap)
                    if key is undefined or entry1.key is undefined:
                        continue
                    elif entry is not entry1:
                        heappush(heap, entry1)
                        return items
                    key = cast(K, entry.key)
                    add_item((key, self.pop(key)))
            except CleanedKeyError as e:
                add_item((e.key, e.value))
            except LookupError:
                pass
        return items


class TLRUDict[K, V](PriorityDict[K, V]):
    __slots__ = ("maxsize", "auto_clean", "priority", "_heap", "_key_to_entry")

    def __init__(
        self, 
        /, 
        priority: Callable[[V], float] = itemgetter(0), # type: ignore
        watermark: Callable[[], float] = time, 
        maxsize: int = 0, 
        auto_clean: bool = True, 
    ):
        super().__init__(priority, watermark, maxsize, auto_clean)


class FastFIFODict[K: Hashable, V](dict[K, V]):
    __slots__ = ("maxsize", "auto_clean")

    def __init__(
        self, 
        /, 
        maxsize: int = 0, 
        auto_clean: bool = True, 
    ):
        self.maxsize = maxsize
        self.auto_clean = auto_clean

    def __repr__(self, /) -> str:
        return super().__repr__()

    def __setitem__(self, key: K, value: V, /):
        super().pop(key, None)
        self.clean(1)
        super().__setitem__(key, value)

    def clean(self, /, extra: int = 0):
        maxsize = self.maxsize
        if self and maxsize > 0:
            remains = maxsize - extra
            if remains <= 0:
                super().clear()
            else:
                pop = super().pop
                while len(self) > remains:
                    try:
                        pop(next(iter(self)), None)
                    except (KeyError, RuntimeError):
                        pass
                    except StopIteration:
                        break

    def popitem(self, /) -> tuple[K, V]:
        try:
            while True:
                try:
                    key = next(iter(self))
                    return key, super().pop(key)
                except (KeyError, RuntimeError):
                    pass
        except StopIteration:
            pass
        raise KeyError(f"{self!r} is empty")

    def setdefault(self, key: K, default: V, /) -> V:
        value = super().setdefault(key, default)
        if self.auto_clean:
            self.clean()
        return value

    def update(self, /, *args, **pairs):
        pop = super().pop
        update = super().update
        for arg in args:
            if arg:
                update(arg)
        update(pairs) # type: ignore
        if self.auto_clean:
            self.clean()


class FastLRUDict[K: Hashable, V](FastFIFODict[K, V]):
    __slots__ = ("maxsize", "auto_clean")

    def __getitem__(self, key: K, /) -> V:
        value = super().pop(key)
        super().__setitem__(key, value)
        return value

    @overload
    def get(self, key: K, /, default: None = None) -> None | V:
        ...
    @overload
    def get[T](self, key: K, /, default: T) -> V | T:
        ...
    def get[T](self, key: K, /, default: None | V | T = None) -> None | V | T:
        try:
            return self[key]
        except KeyError:
            return default

    def setdefault(self, key: K, default: V, /) -> V:
        value = super().pop(key, default)
        self[key] = value
        return value

