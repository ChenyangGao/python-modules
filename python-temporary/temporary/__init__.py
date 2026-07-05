#!/usr/bin/env python3
# coding: utf-8

__author__  = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 1)
__all__ = [
    "temp_attrs", "temp_val", "temp_seq", "temp_set", "temp_map", 
    "temp_col", "temp_globals", "temp_sys_path", 
]

from contextlib import contextmanager
from collections.abc import (
    Callable, Collection, Iterable, Mapping, MutableMapping, 
    MutableSequence, MutableSet, 
)
from copy import copy
from sys import _getframe, path
from typing import Any


@contextmanager
def temp_attrs(obj, /, **attrs):
    set_attrs: dict[str, Any] = {}
    del_attrs: list[str] = []
    add_del_attr = del_attrs.append
    will_setattr = True
    if attrs:
        if hasattr(obj, "__dict__"):
            ns = obj.__dict__
            for attr in attrs:
                try:
                    set_attrs[attr] = ns[attr]
                except KeyError:
                    add_del_attr(attr)
        else:
            if rest_of_keys := attrs.keys() - set(obj.__slots__):
                raise AttributeError(f"{obj!r} have no keys of {rest_of_keys!r}")
            for attr in attrs:
                try:
                    set_attrs[attr] = getattr(obj, attr)
                except AttributeError:
                    add_del_attr(attr)
        for attr, val in attrs.items():
            setattr(obj, attr, val)
    elif hasattr(obj, "__dict__"):
        set_attrs = dict(obj.__dict__)
    elif hasattr(obj, "__slots__"):
        for attr in obj.__slots__:
            try:
                set_attrs[attr] = getattr(obj, attr)
            except AttributeError:
                add_del_attr(attr)
    else:
        will_setattr = False
    try:
        yield obj
    finally:
        if will_setattr:
            if set_attrs:
                for attr, val in set_attrs.items():
                    setattr(obj, attr, val)
            if del_attrs:
                for attr in del_attrs:
                    delattr(obj, attr)
            elif not attrs:
                for attr in obj.__dict__.keys() - set_attrs.keys():
                    try:
                        delattr(obj, attr)
                    except AttributeError:
                        pass


@contextmanager
def temp_val(
    val, 
    /, 
    set: Callable, 
    delete: Callable, 
    get: None | Callable = None, 
):
    del_at_end = True
    if get is not None:
        try:
            val_old = get()
            del_at_end = False
        except (LookupError, ValueError):
            pass
    set(val)
    try:
        yield val
    finally:
        if del_at_end:
            delete()
        else:
            set(val_old)


@contextmanager
def temp_seq[T](
    col: MutableSequence[T], 
    extra: None | Iterable[T] = None, 
    /, 
    copy: None | Callable = copy, 
):
    if copy is None:
        old = tuple(col)
    else:
        old = copy(col)
    if isinstance(col, list):
        clear: Callable[[MutableSequence], Any] = list.clear
        update: Callable[[Any, Iterable[T]], Any] = list.extend
    else:
        clear = MutableSequence.clear
        update = MutableSequence.extend
    if extra:
        update(col, extra)
    try:
        yield col
    finally:
        clear(col)
        update(col, old)


@contextmanager
def temp_set[T](
    col: MutableSet[T], 
    extra: None | Iterable[T] = None, 
    /, 
    copy: None | Callable = copy, 
):
    if copy is None:
        old = tuple(col)
    else:
        old = copy(col)
    if isinstance(col, set):
        clear: Callable[[MutableSet], Any] = set.clear
        update: Callable[[Any, Iterable[T]], Any] = set.update
    else:
        clear = MutableSet.clear
        add = MutableSet.add
        def update(c: MutableSet[T], it: Iterable[T], /):
            for v in it:
                add(c, v)
    if extra:
        update(col, extra)
    try:
        yield col
    finally:
        clear(col)
        update(col, old)


@contextmanager
def temp_map[K, V](
    col: MutableMapping[K, V], 
    extra: None | Iterable[tuple[K, V]] | Mapping[K, V] = None, 
    /, 
    copy: None | Callable = copy, 
):
    if copy is None:
        items: Iterable[tuple[K, V]] = MutableMapping[K, V].items(col) # type: ignore
        old: tuple[tuple[K, V], ...] | MutableMapping[K, V] = tuple(items)
    else:
        old = copy(col)
    if isinstance(col, dict):
        clear: Callable[[MutableMapping], Any] = dict.clear
        update: Callable = dict.update
    else:
        clear = MutableMapping.clear
        update = MutableMapping.update
    try:
        if extra:
            update(col, extra)
        yield col
    finally:
        clear(col)
        update(col, old)


@contextmanager
def temp_col(
    col: Collection, 
    extra = None, 
    /, 
    copy: None | Callable = copy, 
):
    if isinstance(col, MutableSequence):
        yield from temp_seq.__wrapped__(col, extra, copy=copy) # type: ignore
    elif isinstance(col, MutableSet):
        yield from temp_set.__wrapped__(col, extra, copy=copy) # type: ignore
    elif isinstance(col, MutableMapping):
        yield from temp_map.__wrapped__(col, extra, copy=copy) # type: ignore
    else:
        if copy is None:
            old = tuple(col)
        else:
            old = copy(col)
        if extra:
            col.update(extra) # type: ignore
        try:
            yield col
        finally:
            col.clear() # type: ignore
            col.update(old) # type: ignore


@contextmanager
def temp_globals(globals: None | dict = None, /, **ns):
    if globals is None:
        globals = _getframe(2).f_globals
    yield from temp_map.__wrapped__(globals, ns) # type: ignore


@contextmanager
def temp_sys_path(*extra: str):
    yield from temp_seq.__wrapped__(path, extra) # type: ignore

