#!/usr/bin/env python3
# encoding: utf-8

from __future__ import annotations

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 7)
__all__ = [
    "SizedDict", "LIFODict", "FIFODict", "RRDict", "LRUDict", "MRUDict", 
    "TTLDict", "LFUDict", "PriorityDict", "ExpireDict", "TLRUDict", 
    "FastFIFODict", "FastLRUDict", 
]

from collections import deque, defaultdict, UserDict
from collections.abc import Callable, Hashable, Iterable, Iterator
from copy import copy
from dataclasses import dataclass, field
from functools import update_wrapper
from heapq import heappush, heappop, nlargest, nsmallest
from inspect import signature, _empty
from itertools import count
from math import inf, isinf, isnan
from operator import itemgetter
from random import choice
from time import time
from types import MappingProxyType
from typing import cast, overload, Any, Literal, Self
from warnings import warn

from undefined import undefined, Undefined


class CleanedKeyError(KeyError):

    def __init__(self, key, value, /):
        super().__init__(key, value)
        self.key = key
        self.value = value


class SizedDict[K: Hashable, V](defaultdict[K, V]):
    "dictionary with maxsize"
    __slots__ = ("maxsize", "auto_clean", "default_factory")

    def __init__(
        self, 
        /, 
        maxsize: int = 0, 
        auto_clean: bool = True, 
        default_factory: None | Callable[[], V] = None, 
    ):
        super().__init__(default_factory)
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
        maxsize = self.maxsize
        auto_clean = self.auto_clean
        return f"<{cls.__module__}.{cls.__qualname__}({maxsize=!r}, {auto_clean=!r}) object at {hex(id(self))} with {super().__repr__()}>"

    def __contains__(self, key, /) -> bool:
        if self.auto_clean:
            self.clean()
        try:
            super().__getitem__(key)
            return True
        except (KeyError, TypeError):
            return False

    def __setitem__(self, key: K, value: V, /):
        if self.auto_clean and not super().__contains__(key):
            self.clean(1)
        super().__setitem__(key, value)

    @overload
    @classmethod
    def fromkeys[Key](
        cls, 
        it: Iterable[Key], 
        value: None = None, 
        /, 
        **init_kwargs, 
    ) -> SizedDict[Key, Any]:
        ...
    @overload
    @classmethod
    def fromkeys[Key, Val](
        cls, 
        it: Iterable[Key], 
        value: Val, 
        /, 
        **init_kwargs, 
    ) -> SizedDict[Key, Val]:
        ...
    @classmethod
    def fromkeys[Key, Val](
        cls, 
        it: Iterable[Key], 
        value: None | Val = None, 
        /, 
        **init_kwargs, 
    ) -> SizedDict[Key, Any] | SizedDict[Key, Val]:
        d = cast(SizedDict[Key, Any], cls(**init_kwargs))
        for k in it:
            d[k] = value
        return d

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
                    while super().__len__() > remains:
                        add_item(popitem())
                except KeyError:
                    pass
        return items

    def copy(self, /) -> Self:
        return copy(self)

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

    def iter(self, /) -> Iterator[K]:
        if self.auto_clean:
            self.clean()
        return iter(self)

    def items(self, /):
        if self.auto_clean:
            self.clean()
        return super().items()

    def keys(self, /):
        if self.auto_clean:
            self.clean()
        return super().keys()

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
            val = super().__getitem__(key)
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

    def values(self, /):
        if self.auto_clean:
            self.clean()
        return super().values()


class LIFODict[K: Hashable, V](SizedDict[K, V]):
    "Last In First Out (FIFO)"
    __slots__ = ("maxsize", "auto_clean", "default_factory")

    def __setitem__(self, key: K, value: V, /):
        self.discard(key)
        super().__setitem__(key, value)


class FIFODict[K: Hashable, V](LIFODict[K, V]):
    "First In First Out (FIFO)"
    __slots__ = ("maxsize", "auto_clean", "default_factory")

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
    "Random Replacement (RR)"
    __slots__ = ("maxsize", "auto_clean", "default_factory", "_keys", "_key_to_idx")

    def __init__(
        self, 
        /, 
        maxsize: int = 0, 
        auto_clean: bool = True, 
        default_factory: None | Callable[[], V] = None, 
    ):
        super().__init__(maxsize, auto_clean=auto_clean, default_factory=default_factory)
        self._keys: list[K] = []
        self._key_to_idx: dict[K, int] = {}

    def __copy__(self, /) -> Self:
        inst = super().copy()
        inst._keys = copy(self._keys)
        inst._key_to_idx = copy(self._key_to_idx)
        return inst

    def __delitem__(self, key: K, /):
        super().__delitem__(key)
        i = self._key_to_idx.pop(key)
        last_key = self._keys.pop()
        if key is not last_key:
            self._keys[i] = last_key
            self._key_to_idx[last_key] = i

    def __setitem__(self, key: K, value: V, /):
        super().__setitem__(key, value)
        if key not in self._key_to_idx:
            keys = self._keys
            self._key_to_idx[key] = len(keys)
            keys.append(key)

    def popitem(self, /) -> tuple[K, V]:
        key = self.key
        return key, self.pop(key)

    @property
    def key(self, /) -> K:
        try:
            return choice(self._keys)
        except IndexError as e:
            raise KeyError(f"{self!r} is empty") from e

    @property
    def value(self, /) -> V:
        return self[self.key]

    @property
    def item(self, /) -> tuple[K, V]:
        key = self.key
        return key, self[key]


class LRUDict[K: Hashable, V](FIFODict[K, V]):
    "Least Recently Used (LRU)"
    __slots__ = ("maxsize", "auto_clean", "default_factory")

    def __getitem__(self, key: K, /) -> V:
        value = super().__getitem__(key)
        self.discard(key)
        self[key] = value
        return value


@dataclass(slots=True)
class KeyAlive[K]:
    key: K
    is_alive: bool = True

    def __bool__(self, /) -> bool:
        return self.is_alive

    def kill(self, /):
        self.is_alive = False


class MRUDict[K: Hashable, V](SizedDict[K, V]):
    "Most Recently Used (MRU)"
    __slots__ = ("maxsize", "auto_clean", "default_factory", "_key_cache", "_key_deque")

    def __init__(
        self, 
        /, 
        maxsize: int = 0, 
        auto_clean: bool = True, 
        default_factory: None | Callable[[], V] = None, 
    ):
        super().__init__(maxsize, auto_clean=auto_clean, default_factory=default_factory)
        self._key_cache: dict[K, KeyAlive[K]] = {}
        self._key_deque: deque[KeyAlive[K]] = deque()

    def __copy__(self, /) -> Self:
        inst = super().copy()
        inst._key_cache = copy(self._key_cache)
        inst._key_deque = copy(self._key_deque)
        return inst

    def __delitem__(self, key: K, /):
        super().__delitem__(key)
        if key_alive := self._key_cache.pop(key, None):
            key_alive.kill()

    def __getitem__(self, key: K, /) -> V:
        value = super().__getitem__(key)
        if key_alive := self._key_cache.pop(key, None):
            key_alive.kill()
        key_alive = self._key_cache[key] = KeyAlive(key)
        self._key_deque.appendleft(key_alive)
        return value

    def __setitem__(self, key: K, value: V, /):
        super().__setitem__(key, value)
        if key_alive := self._key_cache.pop(key, None):
            key_alive.kill()
        key_alive = self._key_cache[key] = KeyAlive(key)
        self._key_deque.append(key_alive)

    def popitem(self, /) -> tuple[K, V]:
        try:
            pull = self._key_deque.popleft
            while True:
                key_alive = pull()
                try:
                    if key_alive:
                        key = key_alive.key
                        return key, self.pop(key)
                except CleanedKeyError as e:
                    return e.key, e.value
                except (KeyError, RuntimeError):
                    pass
        except IndexError:
            pass
        raise KeyError(f"{self!r} is empty")


class TTLDict[K, V](SizedDict[K, V]):
    "Time-To-Live (TTL)"
    __slots__ = ("maxsize", "auto_clean", "default_factory", "ttl", "is_lru", "_start_time_table")

    def __init__(
        self, 
        /, 
        ttl: float = inf, 
        is_lru: bool = False, 
        maxsize: int = 0, 
        auto_clean: bool = True, 
        default_factory: None | Callable[[], V] = None, 
    ):
        super().__init__(maxsize, auto_clean=auto_clean, default_factory=default_factory)
        self.ttl = ttl
        self.is_lru = is_lru
        self._start_time_table: dict[K, float] = {}

    def __copy__(self, /) -> Self:
        inst = super().copy()
        inst._start_time_table = copy(self._start_time_table)
        return inst

    def __delitem__(self, key: K, /):
        super().__delitem__(key)
        self._start_time_table.pop(key, None)

    def __getitem__(self, key: K, /) -> V:
        value = super().__getitem__(key)
        ttl = self.ttl
        if not (isinf(ttl) or isnan(ttl) or ttl <= 0):
            try:
                expired_time = self._start_time_table[key] + ttl
            except KeyError:
                pass
            else:
                if expired_time <= time():
                    self.discard(key)
                    raise CleanedKeyError(key, value)
        if self.is_lru:
            self.discard(key)
            self[key] = value
        return value

    def __setitem__(self, key: K, value: V, /):
        start_time_table = self._start_time_table
        if self.is_lru:
            self.discard(key)
        else:
            start_time_table.pop(key, None)
        super().__setitem__(key, value)
        start_time_table[key] = time()
        if self.auto_clean:
            self.clean()

    def iter(self, /) -> Iterator[K]:
        ttl = self.ttl
        if isinf(ttl) or isnan(ttl) or ttl <= 0:
            yield from super().iter()
        else:
            discard = self.discard
            watermark = time() - ttl
            for key, start_time in tuple(self._start_time_table.items()):
                if start_time <= watermark:
                    discard(key)
                else:
                    yield key

    @property
    def start_time_table(self, /) -> MappingProxyType:
        return MappingProxyType(self._start_time_table)

    def clean(self, /, extra: int = 0) -> list[tuple[K, V]]:
        items = super().clean(extra)
        ttl = self.ttl
        if self and not (isinf(ttl) or isnan(ttl) or ttl <= 0):
            add_item = items.append
            pop = self.pop
            watermark = time() - ttl
            start_time_items = self._start_time_table.items()
            try:
                while True:
                    try:
                        key, start_time = next(iter(start_time_items))
                        if start_time > watermark:
                            break
                        add_item((key, pop(key)))
                    except CleanedKeyError as e:
                        add_item((e.key, e.value))
                    except (KeyError, RuntimeError):
                        pass
            except StopIteration:
                pass
        return items


class Counter[K: Hashable](UserDict[K, int]):

    def __init__(self, /):
        self.data = {}
        self._heap: list[KeyPriority[int, K]] = []
        self._key_to_entry: dict[K, KeyPriority[int, K]] = {}

    def __missing__(self, _: K, /) -> Literal[0]:
        return 0

    def __delitem__(self, key: K, /):
        super().__delitem__(key)
        if entry := self._key_to_entry.pop(key, None):
            entry.key = undefined

    def __setitem__(self, key: K, value: int, /):
        if value <= 0:
            del self[key]
        else:
            if entry := self._key_to_entry.pop(key, None):
                entry.key = undefined
            super().__setitem__(key, value)
            entry = self._key_to_entry[key] = KeyPriority(value, key)
            heappush(self._heap, entry)

    def max(self, /) -> tuple[K, int]:
        try:
            return max(self.items(), key=itemgetter(1))
        except ValueError as e:
            raise KeyError(f"{self!r} is empty") from e

    def min(self, /) -> tuple[K, int]:
        heap = self._heap
        try:
            while True:
                key = heap[0].key
                if key is not undefined:
                    key = cast(K, key)
                    return key, self[key]
                heappop(heap)
        except IndexError:
            pass
        raise KeyError(f"{self!r} is empty")

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
    "Least Frequently Used (LFU)"
    __slots__ = ("maxsize", "auto_clean", "default_factory", "reset_when_setitem", "reset_when_delitem", "_counter")

    def __init__(
        self, 
        /, 
        maxsize: int = 0, 
        auto_clean: bool = True, 
        reset_when_setitem: bool = False, 
        reset_when_delitem: bool = True, 
        default_factory: None | Callable[[], V] = None, 
    ):
        super().__init__(maxsize, auto_clean=auto_clean, default_factory=default_factory)
        self._counter: Counter[K] = Counter()
        self.reset_when_setitem = reset_when_setitem
        self.reset_when_delitem = reset_when_delitem

    def __copy__(self, /) -> Self:
        inst = super().copy()
        inst._counter = copy(self._counter)
        return inst

    def __delitem__(self, key: K, /):
        super().__delitem__(key)
        if self.reset_when_delitem:
            self._counter.pop(key, None)

    def __getitem__(self, key: K, /) -> V:
        value = super().__getitem__(key)
        self._counter[key] += 1
        return value

    def __setitem__(self, key: K, value: V, /):
        super().__setitem__(key, value)
        if self.reset_when_setitem:
            self._counter[key] = 1
        else:
            self._counter[key] += 1


@dataclass(slots=True, order=True)
class KeyPriority[F, K]:
    priority: F
    key: K | Undefined = field(default=undefined, compare=False)
    _id: int = field(default_factory=count(1).__next__, kw_only=True)


class PriorityDict[K: Hashable, V](SizedDict[K, V]):
    "each value with a priority value"
    __slots__ = ("maxsize", "auto_clean", "default_factory", "prioritize", "watermarker", "is_lru", "_heap", "_key_to_entry")

    def __init__(
        self, 
        /, 
        prioritize: Callable[[K, V], Any] = lambda k, v: 0, 
        watermarker: None | Callable[[], Any] = None, 
        is_lru: bool = False, 
        maxsize: int = 0, 
        auto_clean: bool = True, 
        default_factory: None | Callable[[], V] = None, 
    ):
        super().__init__(maxsize, auto_clean, default_factory=default_factory)
        self.prioritize = prioritize
        self.watermarker = watermarker
        self.is_lru = is_lru
        self._heap: list[KeyPriority] = []
        self._key_to_entry: dict[K, KeyPriority] = {}

    def __copy__(self, /) -> Self:
        inst = super().copy()
        inst._heap = copy(self._heap)
        inst._key_to_entry = copy(self._key_to_entry)
        return inst

    def __delitem__(self, key: K, /):
        super().__delitem__(key)
        self._discard_entry(key)

    def __getitem__(self, key: K) -> V:
        value = super().__getitem__(key)
        if watermarker := self.watermarker:
            priority = self.prioritize(key, value)
            if watermarker() >= priority:
                self.discard(key)
                raise CleanedKeyError(key, value)
        if self.is_lru:
            self.discard(key)
            self[key] = value
        return value

    def __setitem__(self, key: K, value: V, /):
        if self.is_lru:
            self.discard(key)
        super().__setitem__(key, value)
        self._add_entry(key, value)

    def _add_entry(self, key: K, value: V, /) -> KeyPriority:
        self._discard_entry(key)
        entry = self._key_to_entry[key] = KeyPriority(self.prioritize(key, value), key)
        heappush(self._heap, entry)
        return entry

    def _discard_entry(self, /, key: K) -> None | KeyPriority:
        if entry := self._key_to_entry.pop(key, None):
            entry.key = undefined
        return entry

    def _pop_entry(self, /) -> KeyPriority:
        heap = self._heap
        while heap:
            entry = heappop(heap)
            key = entry.key
            if key is not undefined:
                self._key_to_entry.pop(cast(K, key), None)
                return entry
        raise KeyError("pop from an empty priority queue")

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
        items = super().clean(extra)
        add_item = items.append
        if watermarker := self.watermarker:
            heap = self._heap
            watermark = watermarker()
            try:
                while True:
                    entry = heap[0]
                    key = entry.key
                    if key is not undefined and watermark < entry.priority:
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


class ExpireDict[K, V](PriorityDict[K, V]):
    "each value with an expiration timestamp"
    __slots__ = ("maxsize", "auto_clean", "default_factory", "prioritize", "watermarker", "is_lru", "_heap", "_key_to_entry")

    def __init__(
        self, 
        /, 
        expire_timer: float | Callable[[K, V], float] = lambda _, v, /: v[0], # type: ignore
        watermarker: float | Callable[[], float] = time, 
        is_lru: bool = False, 
        maxsize: int = 0, 
        auto_clean: bool = True, 
        default_factory: None | Callable[[], V] = None, 
    ):
        if isinstance(expire_timer, (int, float)) or not callable(expire_timer):
            ttl = expire_timer
            expire_timer = lambda *_: time() + ttl
        if isinstance(watermarker, (int, float)) or not callable(watermarker):
            offset = watermarker
            watermarker = lambda: time() + offset
        super().__init__(
            prioritize=expire_timer, 
            watermarker=watermarker, 
            is_lru=is_lru, 
            maxsize=maxsize, 
            auto_clean=auto_clean, 
            default_factory=default_factory, 
        )


TLRUDict = ExpireDict

# TODO: ARCDict: Adaptive Replacement Cache (CC), LRU -> LFU
# TODO: TwoQDict: Two Queues (2Q), FIFO -> LRU
# TODO: LRUKDict: Least Recently Used at least K times (LRU-K), LFU -> LRU
# TODO: LFU with each item add_time, use (add_time, count) to calculate a priority value
# TODO: LFU with each item access_time, use (access_time, count) to calculate a priority value

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

