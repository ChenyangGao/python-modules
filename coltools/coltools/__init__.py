#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 1)
__all__ = ["add", "clear", "discard", "pop", "remove", "update"]

from collections.abc import Iterable, MutableMapping, MutableSequence, MutableSet
from typing import Any


def add[T: Any](c: T, v, /) -> T:
    try:
        c.add(v)
    except (AttributeError, TypeError):
        if isinstance(c, MutableSequence):
            if isinstance(c, list):
                c.append(v)
            else:
                MutableSequence.append(c, v)
        elif isinstance(c, MutableMapping):
            if isinstance(v, tuple) and len(v) == 2:
                k, v = v
            else:
                k = v
            c[k] = v
        else:
            raise
    return c


def clear[T: Any](c: T, /) -> T:
    try:
        c.clear()
    except (AttributeError, TypeError):
        if isinstance(c, MutableSequence):
            MutableSequence.clear(c)
        elif isinstance(c, MutableSet):
            MutableSet.clear(c)
        elif isinstance(c, MutableMapping):
            MutableMapping.clear(c)
        else:
            raise
    return c


def discard[T: Any](c: T, k, /) -> T:
    try:
        c.discard(k)
    except (AttributeError, TypeError):
        if isinstance(c, MutableSequence):
            try:
                MutableSequence.remove(c, k)
            except IndexError:
                pass
        elif isinstance(c, MutableSet):
            MutableSet.discard(c, k)
        elif isinstance(c, MutableMapping):
            if isinstance(c, dict):
                c.pop(k, None)
            else:
                MutableMapping.pop(c, k, None)
        else:
            raise
    return c


def pop[T: Any](c: T, /):
    try:
        if isinstance(c, MutableMapping):
            return c.popitem()
        else:
            return c.pop()
    except (AttributeError, TypeError):
        if isinstance(c, MutableSequence):
            return MutableSequence.pop(c)
        elif isinstance(c, MutableSet):
            return MutableSet.pop(c)
        elif isinstance(c, MutableMapping):
            return MutableMapping.popitem(c)
        else:
            raise


def remove[T: Any](c: T, k, /) -> T:
    try:
        c.remove(k)
    except (AttributeError, TypeError):
        if isinstance(c, MutableSequence):
            MutableSequence.remove(c, k)
        elif isinstance(c, MutableSet):
            MutableSet.remove(c, k)
        elif isinstance(c, MutableMapping):
            if isinstance(c, dict):
                c.pop(k)
            else:
                MutableMapping.pop(c, k)
        else:
            raise
    return c


def update[T: Any](c: T, it: Iterable, /) -> T:
    if it:
        try:
            c.update(it)
        except (AttributeError, TypeError):
            if isinstance(c, MutableSequence):
                if isinstance(c, list):
                    c.extend(it)
                else:
                    MutableSequence.extend(c, it)
            elif isinstance(c, MutableSet):
                add = MutableSet.add
                for v in it:
                    add(c, v)
            elif isinstance(c, MutableMapping):
                MutableMapping.update(c, it)
            else:
                raise
    return c

