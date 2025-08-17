#!/usr/bin/env python3
# coding: utf-8

__author__  = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 1)
__all__ = [
    "temp_attr", "temp_val", "temp_seq", "temp_set", "temp_map", 
    "temp_col", "temp_globals", "temp_sys_path", 
]

from contextlib import contextmanager
from collections.abc import (
    Callable, Collection, Iterable, Mapping, MutableMapping, 
    MutableSequence, MutableSet, 
)
from copy import copy
from sys import _getframe, path

from dicttools import clear, update, iter_items


@contextmanager
def temp_attr(obj, attr, val, /):
    try:
        val_old = getattr(obj, attr)
        set_at_end = True
    except AttributeError:
        set_at_end = False
    setattr(obj, attr, val)
    try:
        yield val
    finally:
        if set_at_end:
            setattr(obj, attr, val_old)
        else:
            delattr(obj, attr)


@contextmanager
def temp_val(val, set, get=None, del_=None):
    set_at_end = True
    if get is not None:
        try:
            val_old = get()
        except:
            if del_ is None:
                raise
            set_at_end = False
    elif del_ is None:
        val_old = set(val)
    else:
        set(val)
        set_at_end = False
    try:
        yield val
    finally:
        if set_at_end:
            set(val_old)
        else:
            del_()


@contextmanager
def temp_seq[T](
    c: MutableSequence[T], 
    extra: None | Iterable[T] = None, 
    /, 
    copy: Callable = copy, 
):
    if copy is None:
        old = list(c)
    else:
        old = copy(c)
    if extra:
        seq_update(s, extra)
    try:
        yield c
    finally:
        seq_clear(c)
        seq_update(c, old)


@contextmanager
def temp_set[T](
    s: MutableSet[T], 
    extra: None | Iterable[T] = None, 
    /, 
    copy: None | Callable = copy, 
):
    if copy is None:
        old = list(s)
    else:
        old = copy(s)
    if extra:
        set_update(s, extra)
    try:
        yield s
    finally:
        set_clear(s)
        set_update(s, old)


@contextmanager
def temp_map[K, V](
    m: MutableMapping[K, V], 
    extra: None | Iterable[tuple[K, V]] | Mapping[K, V] = None, 
    /, 
    copy: None | Callable = copy, 
):
    if copy is None:
        old = list(iter_items(m))
    else:
        old = copy(m)
    try:
        if extra:
            update(m, extra)
        yield m
    finally:
        clear(m)
        update(m, old)


@contextmanager
def temp_col(
    c: Collection, 
    extra = None, 
    /, 
    copy: None | Callable = copy, 
):
    if isinstance(c, MutableSequence):
        yield from temp_seq.__wrapped__(c, extra, copy=copy)
    elif isinstance(c, MutableSet):
        yield from temp_set.__wrapped__(c, extra, copy=copy)
    elif isinstance(c, MutableMapping):
        yield from temp_map.__wrapped__(c, extra, copy=copy)
    else:
        if copy is None:
            old = list(c)
        else:
            old = copy(c)
        if extra:
            c.update(extra)
        try:
            yield c
        finally:
            c.clear()
            c.update(c, old)


@contextmanager
def temp_globals(globals: None | dict = None, /, **ns):
    if globals is None:
        globals = _getframe(2).globals
    yield from temp_map.__wrapped__(globals, ns)


@contextmanager
def temp_sys_path(*extra: str):
    yield from temp_seq.__wrapped__(path, extra)

