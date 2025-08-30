#!/usr/bin/env python3
# encoding: utf-8

from __future__ import annotations

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 5)
__all__ = [
    "get", "get_first", "get_first_item", "get_all", "get_all_items", 
    "pop", "pop_first", "pop_first_item", "pop_all", "pop_all_items", 
    "popitem", "setdefault", "setdefault_first", "setdefault_first_item", 
    "setdefault_all", "setdefault_all_items", "discard", "discard_first", 
    "discard_all", "contains", "contains_first", "all_contains", 
    "contains_value", "contains_filter", "contains_any", "update", "merge", 
    "chain_get", "keyof", "clear", "keys", "values", "items", "iter_keys", 
    "iter_values", "iter_items", "dict_swap", "dict_map", "iter_items_map", 
    "dict_group", "dict_merge", "dict_update", "dict_key_to_lower_merge", 
    "dict_key_to_lower_update", "KeyedDict", "KeyLowerDict", 
]

from collections.abc import (
    Callable, ItemsView, Iterable, Iterator, KeysView, Mapping, 
    MutableMapping, ValuesView, 
)
from functools import partial
from itertools import chain
from operator import methodcaller
from typing import cast, overload, Any, Protocol, Self

from undefined import undefined, Undefined


_null = object()


class SupportsLower(Protocol):
    def lower(self, /) -> Self:
        ...


def _lower[T](o: T, /) -> T:
    try:
        return getattr(o, "lower")()
    except Exception:
        return o


def _hash_eq(x, y, hash_of_x=undefined, /) -> bool:
    try:
        if hash_of_x is undefined:
            return x is y or hash(x) == hash(y)
        else:
            return x is y or hash_of_x == hash(y)
    except Exception:
        return False


@overload
def get[K, V](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    k, 
    /, 
    default: Undefined = undefined, 
) -> V:
    ...
@overload
def get[K, V, V2](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    k, 
    /, 
    default: V2, 
) -> V | V2:
    ...
def get[K, V, V2](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    k, 
    /, 
    default: Undefined | V2 = undefined, 
) -> V | V2:
    if isinstance(m, Mapping):
        try:
            return m[k]
        except (LookupError, TypeError):
            pass
    else:
        try:
            kh = hash(k)
        except Exception:
            for key, val in m:
                if key is k:
                    return val
        else:
            for key, val in m:
                if _hash_eq(k, key, kh):
                    return val
    if default is undefined:
        raise KeyError(k)
    return cast(V2, default)


@overload
def get_first[K, V](
    m: Mapping[K, V], 
    /, 
    *keys, 
    default: Undefined = undefined, 
) -> V:
    ...
@overload
def get_first[K, V, V2](
    m: Mapping[K, V], 
    /, 
    *keys, 
    default: V2, 
) -> V | V2:
    ...
def get_first[K, V, V2](
    m: Mapping[K, V], 
    /, 
    *keys, 
    default: Undefined | V2 = undefined, 
) -> V | V2:
    for k in keys:
        try:
            return m[k]
        except (LookupError, TypeError):
            pass
    if default is undefined:
        raise KeyError(*keys)
    return cast(V2, default)


@overload
def get_first_item[K, V](
    m: Mapping[K, V], 
    /, 
    *keys, 
    default: Undefined = undefined, 
) -> tuple[K, V]:
    ...
@overload
def get_first_item[K, V, K2, V2](
    m: Mapping[K, V], 
    /, 
    *keys: K2, 
    default: V2, 
) -> tuple[K2, V | V2]:
    ...
def get_first_item[K, V, K2, V2](
    m: Mapping[K, V], 
    /, 
    *keys: K2, 
    default: Undefined | V2 = undefined, 
) -> tuple[K, V] | tuple[K2, V | V2]:
    for k in keys:
        try:
            return k, m[cast(K, k)]
        except (LookupError, TypeError):
            pass
    if default is undefined:
        raise KeyError(*keys)
    return k, cast(V2, default)


@overload
def get_all[K, V](
    m: Mapping[K, V], 
    /, 
    *keys, 
    default: Undefined = undefined, 
) -> list[V]:
    ...
@overload
def get_all[K, V, V2](
    m: Mapping[K, V], 
    /, 
    *keys, 
    default: V2, 
) -> list[V | V2]:
    ...
def get_all[K, V, V2](
    m: Mapping[K, V], 
    /, 
    *keys, 
    default: Undefined | V2 = undefined, 
) -> list[V] | list[V | V2]:
    if default is undefined:
        return [m[k] for k in keys if k in m]
    return [get(m, k, default) for k in keys]


@overload
def get_all_items[K, V](
    m: Mapping[K, V], 
    /, 
    *keys, 
    default: Undefined = undefined, 
) -> list[tuple[K, V]]:
    ...
@overload
def get_all_items[K, V, K2, V2](
    m: Mapping[K, V], 
    /, 
    *keys: K2, 
    default: V2, 
) -> list[tuple[K2, V | V2]]:
    ...
def get_all_items[K, V, K2, V2](
    m: Mapping[K, V], 
    /, 
    *keys: K2, 
    default: Undefined | V2 = undefined, 
) -> list[tuple[K, V]] | list[tuple[K2, V | V2]]:
    if default is undefined:
        return [(cast(K, k), m[cast(K, k)]) for k in keys if k in m]
    return [(k, get(m, k, default)) for k in keys]


@overload
def pop[K, V](
    m: MutableMapping[K, V], 
    k, 
    /, 
    default: Undefined = undefined, 
) -> V:
    ...
@overload
def pop[K, V, V2](
    m: MutableMapping[K, V], 
    k, 
    /, 
    default: V2, 
) -> V | V2:
    ...
def pop[K, V, V2](
    m: MutableMapping[K, V], 
    k, 
    /, 
    default: Undefined | V2 = undefined, 
) -> V | V2:
    if isinstance(m, dict):
        if default is undefined:
            return m.pop(k)
        return m.pop(k, cast(V2, default))
    v = get(m, k, _null)
    if v is _null:
        if default is undefined:
            raise KeyError(k)
        return cast(V2, default)
    else:
        del m[cast(K, k)]
        return cast(V, v)


@overload
def pop_first[K, V](
    m: MutableMapping[K, V], 
    /, 
    *keys, 
    default: Undefined = undefined, 
) -> V:
    ...
@overload
def pop_first[K, V, V2](
    m: MutableMapping[K, V], 
    /, 
    *keys, 
    default: V2, 
) -> V | V2:
    ...
def pop_first[K, V, V2](
    m: MutableMapping[K, V], 
    /, 
    *keys, 
    default: Undefined | V2 = undefined, 
) -> V | V2:
    for k in keys:
        try:
            return pop(m, k)
        except KeyError:
            pass
    if default is undefined:
        raise KeyError(*keys)
    return cast(V2, default)


@overload
def pop_first_item[K, V](
    m: MutableMapping[K, V], 
    /, 
    *keys, 
    default: Undefined = undefined, 
) -> tuple[K, V]:
    ...
@overload
def pop_first_item[K, V, K2, V2](
    m: MutableMapping[K, V], 
    /, 
    *keys: K2, 
    default: V2, 
) -> tuple[K2, V | V2]:
    ...
def pop_first_item[K, V, K2, V2](
    m: MutableMapping[K, V], 
    /, 
    *keys: K2, 
    default: Undefined | V2 = undefined, 
) -> tuple[K, V] | tuple[K2, V | V2]:
    for k in keys:
        try:
            return cast(K, k), pop(m, cast(K, k))
        except KeyError:
            pass
    if default is undefined:
        raise KeyError(*keys)
    return k, cast(V2, default)


@overload
def pop_all[K, V](
    m: MutableMapping[K, V], 
    /, 
    *keys, 
    default: Undefined = undefined, 
) -> list[V]:
    ...
@overload
def pop_all[K, V, V2](
    m: MutableMapping[K, V], 
    /, 
    *keys, 
    default: V2, 
) -> list[V | V2]:
    ...
def pop_all[K, V, V2](
    m: MutableMapping[K, V], 
    /, 
    *keys, 
    default: Undefined | V2 = undefined, 
) -> list[V] | list[V | V2]:
    if default is undefined:
        return [pop(m, k) for k in keys if k in m]
    default = cast(V2, default)
    return [pop(m, k, default) for k in keys] # type: ignore


@overload
def pop_all_items[K, V](
    m: MutableMapping[K, V], 
    /, 
    *keys, 
    default: Undefined = undefined, 
) -> list[tuple[K, V]]:
    ...
@overload
def pop_all_items[K, V, K2, V2](
    m: MutableMapping[K, V], 
    /, 
    *keys: K2, 
    default: V2, 
) -> list[tuple[K2, V | V2]]:
    ...
def pop_all_items[K, V, K2, V2](
    m: MutableMapping[K, V], 
    /, 
    *keys: K2, 
    default: Undefined | V2 = undefined, 
) -> list[tuple[K, V]] | list[tuple[K2, V | V2]]:
    if default is undefined:
        return [(cast(K, k), pop(m, cast(K, k))) for k in keys if k in m]
    default = cast(V2, default)
    return [(k, pop(m, k, default)) for k in keys] # type: ignore


def popitem[K, V](m: MutableMapping[K, V], /) -> tuple[K, V]:
    try:
        return m.popitem()
    except (AttributeError, TypeError):
        while m:
            try:
                k = next(iter(m))
                v = pop(m, k)
                return k, v
            except KeyError:
                pass
        raise KeyError(f"mutable mapping {m!r} is empty")


def setdefault[K, V](
    m: MutableMapping[K, V], 
    k: K, 
    v: V, 
    /, 
) -> V:
    if isinstance(m, dict):
        return m.setdefault(k, v)
    try:
        return m[k]
    except LookupError:
        m[k] = v
        return v


def setdefault_first[K, V](
    m: MutableMapping[K, V], 
    /, 
    *keys: K, 
    default: V, 
) -> bool:
    for k in keys:
        if k not in m:
            m[k] = default
            return True
    return False


def setdefault_first_item[K, V](
    m: MutableMapping[K, V], 
    /, 
    *keys: K, 
    default: V, 
) -> None | tuple[K, V]:
    for k in keys:
        if k not in m:
            m[k] = default
            return k, default
    return None


def setdefault_all[K, V](
    m: MutableMapping[K, V], 
    /, 
    *keys: K, 
    default: V, 
) -> list[V]:
    return [setdefault(m, k, default) for k in keys]


def setdefault_all_items[K, V](
    m: MutableMapping[K, V], 
    /, 
    *keys, 
    default: V, 
) -> list[tuple[K, V]]:
    return [(k, setdefault(m, k, default)) for k in keys]


def discard(m: MutableMapping, k, /) -> bool:
    try:
        pop(m, k)
        return True
    except KeyError:
        return False


def discard_first[K, V](m: MutableMapping[K, V], /, *keys) -> None | K:
    for k in keys:
        if pop(m, k, _null) is not _null:
            return k
    return None


def discard_all[K, V](m: MutableMapping[K, V], /, *keys) -> list[K]:
    return [k for k in keys if pop(m, k, _null) is not _null]


def contains(m: Mapping, k, /) -> bool:
    try:
        return k in m
    except TypeError:
        return False


def contains_first[K, V](m: Mapping[K, V], /, *keys) -> None | K:
    for k in keys:
        if contains(m, k):
            return k
    return None


def contains_filter[K, V](m: Mapping[K, V], /, *keys) -> list[K]:
    return [k for k in keys if contains(m, k)]


def contains_value(m: Mapping, v, /) -> bool:
    return v in values(m)


def contains_all[K, V](m: Mapping[K, V], /, *keys) -> bool:
    return all(contains(m, k) for k in keys)


def contains_any[K, V](m: Mapping[K, V], /, *keys) -> bool:
    return any(contains(m, k) for k in keys)


def update[M: MutableMapping](
    self: M, 
    other: Mapping | Iterable[tuple[Any, Any]], 
    /, 
    recursive: bool = False, 
) -> M:
    if recursive:
        for k, v in iter_items(other):
            if isinstance(v, Mapping) and k in self and isinstance((v0 := self[k]), MutableMapping):
                update(v0, v, recursive=True)
            else:
                self[k] = v
    elif isinstance(self, dict):
        self.update(other)
    else:
        for k, v in iter_items(other):
            self[k] = v
    return self


def merge[M: MutableMapping](
    self: M, 
    other: Mapping | Iterable[tuple[Any, Any]], 
    /, 
    recursive: bool = False, 
) -> M:
    if recursive:
        for k, v in iter_items(other):
            if k in self:
                v0 = self[k]
                if isinstance(v, Mapping) and isinstance(v0, MutableMapping):
                    merge(v0, v, recursive=True)
            else:
                self[k] = v
    elif isinstance(self, dict):
        setdefault = self.setdefault
        for k, v in iter_items(other):
            setdefault(k, v)
    else:
        for k, v in iter_items(other):
            if k not in self:
                self[k] = v
    return self


def chain_get(
    m: Mapping, 
    /, 
    *keys, 
    default=undefined, 
):
    try:
        d: Any = m
        for k in keys:
            d = d[k]
        return d
    except (TypeError, LookupError) as e:
        if default is undefined:
            raise KeyError(keys) from e
        return default


def keyof[K, V](m: Mapping[K, V] | Iterable[tuple[K, V]], v, /) -> K:
    try:
        vh = hash(v)
    except Exception:
        for key, val in iter_items(m):
            if v is val:
                return key
    else:
        for key, val in iter_items(m):
            if _hash_eq(v, val, vh):
                return key
    raise ValueError(f"{m!r} has no key to value {v!r}")


def clear(m: MutableMapping, /):
    try:
        m.clear()
        if not m:
            return
    except (AttributeError, TypeError):
        pass
    ks = keys(m)
    try:
        while True:
            pop(m, next(iter(ks)), None)
    except StopIteration:
        pass


def keys[K, V](m: Mapping[K, V], /) -> KeysView[K]:
    try:
        if isinstance((keys := m.keys()), KeysView):
            return keys
    except (AttributeError, TypeError):
        pass
    return KeysView(m)


def values[K, V](m: Mapping[K, V], /) -> ValuesView[V]:
    try:
        if isinstance((values := m.values()), ValuesView):
            return values
    except (AttributeError, TypeError):
        pass
    return ValuesView(m)


def items[K, V](m: Mapping[K, V], /) -> ItemsView[K, V]:
    try:
        if isinstance((items := m.items()), ItemsView):
            return items
    except (AttributeError, TypeError):
        pass
    return ItemsView(m)


def iter_keys[K, V](m: Mapping[K, V] | Iterable[tuple[K, V]], /) -> Iterator[K]:
    if isinstance(m, Mapping):
        return iter(m)
    return (t[0] for t in m)


def iter_values[K, V](m: Mapping[K, V] | Iterable[tuple[K, V]], /) -> Iterator[V]:
    if isinstance(m, Mapping):
        return iter(values(m))
    return (t[1] for t in m)


def iter_items[K, V](m: Mapping[K, V] | Iterable[tuple[K, V]], /) -> Iterator[tuple[K, V]]:
    if isinstance(m, Mapping):
        return iter(items(m))
    return iter(m)


def dict_swap[K, V](m: Mapping[K, V] | Iterable[tuple[K, V]], /) -> dict[V, K]:
    return {v: k for k, v in iter_items(m)}


@overload
def dict_map[K, V](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *, 
    key: None = None, 
    value: None = None, 
) -> dict[K, V]:
    ...
@overload
def dict_map[K, V, K2](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *, 
    key: Callable[[K], K2] | Mapping[K, K2], 
    value: None = None, 
) -> dict[K2, V]:
    ...
@overload
def dict_map[K, V, V2](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *, 
    key: None = None, 
    value: Callable[[V], V2] | Mapping[V, V2], 
) -> dict[K, V2]:
    ...
@overload
def dict_map[K, V, K2, V2](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *, 
    key: Callable[[K], K2] | Mapping[K, K2], 
    value: Callable[[V], V2] | Mapping[V, V2], 
) -> dict[K2, V2]:
    ...
def dict_map[K, V, K2, V2](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    key: None | Callable[[K], K2] | Mapping[K, K2] = None, 
    value: None | Callable[[V], V2] | Mapping[V, V2] = None, 
) -> dict[K, V] | dict[K2, V] | dict[K, V2] | dict[K2, V2]:
    def get(m, k, /):
        try:
            return m[k]
        except LookupError:
            return k
    if isinstance(key, Mapping):
        key = partial(get, key)
    if isinstance(value, Mapping):
        value = partial(get, value)
    if key is None:
        if value is None:
            return dict(m)
        return {k: value(v) for k, v in iter_items(m)}
    elif value is None:
        return {key(k): v for k, v in iter_items(m)}
    else:
        return {key(k): value(v) for k, v in iter_items(m)}


@overload
def iter_items_map[K, V](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *, 
    key: None = None, 
    value: None = None, 
) -> Iterator[tuple[K, V]]:
    ...
@overload
def iter_items_map[K, V, K2](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *, 
    key: Callable[[K], K2] | Mapping[K, K2], 
    value: None = None, 
) -> Iterator[tuple[K2, V]]:
    ...
@overload
def iter_items_map[K, V, V2](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *, 
    key: None = None, 
    value: Callable[[V], V2] | Mapping[V, V2], 
) -> Iterator[tuple[K, V2]]:
    ...
@overload
def iter_items_map[K, V, K2, V2](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *, 
    key: Callable[[K], K2] | Mapping[K, K2], 
    value: Callable[[V], V2] | Mapping[V, V2], 
) -> Iterator[tuple[K2, V2]]:
    ...
def iter_items_map[K, V, K2, V2](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    key: None | Callable[[K], K2] | Mapping[K, K2] = None, 
    value: None | Callable[[V], V2] | Mapping[V, V2] = None, 
) -> Iterator[tuple[K, V]] | Iterator[tuple[K2, V]] | Iterator[tuple[K, V2]] | Iterator[tuple[K2, V2]]:
    def get(m, k, /):
        try:
            return m[k]
        except LookupError:
            return k
    if isinstance(key, Mapping):
        key = partial(get, key)
    if isinstance(value, Mapping):
        value = partial(get, value)
    if key is None:
        if value is None:
            return iter_items(m)
        return ((k, value(v)) for k, v in iter_items(m))
    elif value is None:
        return ((key(k), v) for k, v in iter_items(m))
    else:
        return ((key(k), value(v)) for k, v in iter_items(m))


@overload
def dict_group[K, V](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *, 
    key: None = None, 
    value: None = None, 
) -> dict[K, list[V]]:
    ...
@overload
def dict_group[K, V, K2](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *, 
    key: Callable[[K], K2] | Mapping[K, K2], 
    value: None = None, 
) -> dict[K2, list[V]]:
    ...
@overload
def dict_group[K, V, V2](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *, 
    key: None = None, 
    value: Callable[[V], V2] | Mapping[V, V2], 
) -> dict[K, list[V2]]:
    ...
@overload
def dict_group[K, V, K2, V2](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *, 
    key: Callable[[K], K2] | Mapping[K, K2], 
    value: Callable[[V], V2] | Mapping[V, V2], 
) -> dict[K2, list[V2]]:
    ...
def dict_group[K, V, K2, V2](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    key: None | Callable[[K], K2] | Mapping[K, K2] = None, 
    value: None | Callable[[V], V2] | Mapping[V, V2] = None, 
) -> dict[K, list[V]] | dict[K2, list[V]] | dict[K, list[V2]] | dict[K2, list[V2]]:
    d: dict = {}
    for k, v in iter_items_map(m, key=key, value=value):
        try:
            d[k].append(v)
        except KeyError:
            d[k] = [v]
    return d


def dict_merge[K, V](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *ms: Mapping[K, V] | Iterable[tuple[K, V]], 
    **kwds, 
) -> dict[K, V]:
    d: dict[K, V]
    if isinstance(m, dict):
        d = m
    else:
        d = dict(m)
    setdefault = d.setdefault
    k: Any
    if ms:
        for k, v in chain.from_iterable(map(iter_items, ms)):
            setdefault(k, v)
    if kwds:
        for k, v in kwds.items():
            setdefault(k, v)
    return d


def dict_update[K, V](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *ms: Mapping[K, V] | Iterable[tuple[K, V]], 
    **kwds, 
) -> dict[K, V]:
    d: dict[K, V]
    if isinstance(m, dict):
        d = m
    else:
        d = dict(m)
    update = d.update
    if ms:
        for m in ms:
            update(m)
    if kwds:
        update(cast(dict[K, V], kwds))
    return d


def dict_key_to_lower_merge[K, V](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *ms: Mapping[K, V] | Iterable[tuple[K, V]], 
    **kwds, 
) -> dict[K, V]:
    key_to_lower = partial(iter_items_map, key=_lower)
    return dict_merge(
        {}, 
        key_to_lower(m), 
        *map(key_to_lower, ms), 
        key_to_lower(kwds), 
    )


def dict_key_to_lower_update[K, V](
    m: Mapping[K, V] | Iterable[tuple[K, V]], 
    /, 
    *ms: Mapping[K, V] | Iterable[tuple[K, V]], 
    **kwds, 
) -> dict[K, V]:
    key_to_lower = partial(iter_items_map, key=_lower)
    return dict_update(
        {}, 
        key_to_lower(m), 
        *map(key_to_lower, ms), 
        key_to_lower(kwds), 
    )


class KeyedDict[K, V, K2](dict[K2, V]):
    __key__: Callable[[K2], K] = lambda x: x # type: ignore

    def __contains__(self, key, /) -> bool:
        try:
            return super().__contains__(type(self).__key__(key))
        except TypeError:
            return False

    def __delitem__(self, key: K2, /):
        return super().__delitem__(type(self).__key__(key))

    def __getitem__(self, key: K2, /) -> V:
        return super().__getitem__(type(self).__key__(key))

    def __setitem__(self, key: K2, value: V, /):
        return super().__setitem__(type(self).__key__(key), value)

    def get(self, key: K2, /, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def pop(self, key: K2, /, default=undefined):
        k = type(self).__key__(key)
        if default is undefined:
            return super().pop(k)
        else:
            return super().pop(k, default)

    def setdefault(self, key: K2, /, default: V) -> V:
        return super().setdefault(type(self).__key__(key), default)

    def update(self, /, *args, **kwds):
        key = type(self).__key__
        arg: None | Mapping[K2, V] | Iterable[tuple[K2, V]]
        for arg in args:
            if arg:
                for k, v in iter_items(arg):
                    self[key(k)] = v
        if kwds:
            for k, v in kwds.items():
                self[key(k)] = v


class KeyLowerDict[K: SupportsLower, V](KeyedDict[K, V, K]):
    __key__ = methodcaller("lower")

