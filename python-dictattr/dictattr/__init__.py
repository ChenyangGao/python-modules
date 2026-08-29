#!/usr/bin/env python3
# encoding: utf-8

from __future__ import annotations

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 5)
__all__ = [
    "AttrDict", "MapAttr", "MuMapAttr", "DictAttr", "UserDictAttr", "ChainUserDictAttr", 
    "IntMapAttr", "IntMuMapAttr", "StrMapAttr", "StrMuMapAttr", 
]

from collections import UserDict
from collections.abc import Iterator, Mapping, MutableMapping
from typing import overload, Any, Self, Generic
from typing import _GenericAlias # type: ignore


class DictAttrMixin[K, V](MutableMapping[K, V]):

    def __delattr__(self, name: K, /): # type: ignore
        try:
            del self[name]
        except KeyError as e:
            raise AttributeError(name) from e

    def __getattr__(self, name: K, /) -> V: # type: ignore
        try:
            return self[name]
        except KeyError as e:
            raise AttributeError(name) from e

    def __setattr__(self, name: K, value: V, /): # type: ignore
        try:
            self[name] = value
        except KeyError as e:
            raise AttributeError(name) from e


class MapAttrMixin[K, V](Mapping[K, V]):
    __dict__: dict[K, V] # type: ignore

    def __contains__(self, key, /) -> bool:
        return key in self.__dict__

    def __getitem__(self, key: K, /) -> V:
        return self.__dict__[key]

    def __iter__(self, /) -> Iterator[K]:
        return iter(self.__dict__)

    def __len__(self, /) -> int:
        return len(self.__dict__)


class MuMapAttrMixin[K, V](MapAttrMixin[K, V], MutableMapping[K, V]):

    def __delitem__(self, key: K, /):
        del self.__dict__[key]

    def __setitem__(self, key: K, val: V, /):
        self.__dict__[key] = val


class ValueMapAttrMixin[K, V](MapAttrMixin[K, V]):

    def __new__(cls, value=None, /, *args, **kwds):
        if isinstance(value, (tuple, list, Mapping)):
            args = value, *args
            value = None
        for a in args:
            kwds.update(a)
        if value is None:
            value = kwds.get("id")
        base: Any
        for base in reversed(cls.__bases__):
            if not (base is Generic or isinstance(base, _GenericAlias)):
                break
        self = base.__new__(cls, value or 0)
        if kwds:
            self.__dict__.update(kwds)
        return self

    def __repr__(self, /) -> str:
        cls = type(self)
        return f"{cls.__module__}.{cls.__qualname__}({super().__repr__()}, {self.__dict__!r})"


class ValueMuMapAttrMixin[K, V](ValueMapAttrMixin[K, V], MuMapAttrMixin[K, V]):
    pass


class AttrDict[K, V](dict[K, V]):

    def __init__(self, /, *args, **kwds):
        super().__init__(*args, **kwds)
        self.__dict__ = self # type: ignore


class MapAttr[K, V](MapAttrMixin[K, V]):

    def __init__(self, /, *args, **kwds):
        self.__dict__: dict[K, V] # type: ignore
        self.__dict__.update(*args, **kwds)

    def __repr__(self, /) -> str:
        cls = type(self)
        return f"{cls.__module__}.{cls.__qualname__}({self.__dict__})"

    @classmethod
    def of(
        cls, 
        d: None | dict[K, V] = None, 
        /, 
    ) -> Self:
        self = cls.__new__(cls)
        if d is not None:
            self.__dict__ = d
        return self


class MuMapAttr[K, V](MapAttr[K, V], MuMapAttrMixin[K, V]):
    pass


class DictAttr[K, V](DictAttrMixin[K, V], dict[K, V]):
    pass


class UserDictAttr[K, V](DictAttrMixin[K, V], UserDict[K, V]):

    def __getitem__(self, key, /):
        d = super().__getitem__(key)
        if isinstance(d, Mapping) and not isinstance(d, __class__): # type: ignore
            return type(self)(d)
        return d

    def __repr__(self, /) -> str:
        cls = type(self)
        return f"{cls.__module__}.{cls.__qualname__}.of({self.data!r})"

    @classmethod
    def of(cls, m: Mapping, /) -> Self:
        self = cls()
        self.__dict__["data"] = m
        return self


class ChainDictAttr[K, V](UserDictAttr[K, V | "ChainDictAttr"]):

    def __getitem__(self, key, /) -> V | ChainDictAttr:
        try:
            return super().__getitem__(key)
        except KeyError:
            d = self.__dict__[key] = type(self)()
            return d


class IntMapAttr[K, V](ValueMapAttrMixin[K, V], int):
    pass


class IntMuMapAttr[K, V](ValueMuMapAttrMixin[K, V], int):
    pass


class StrMapAttr[K, V](ValueMapAttrMixin[K, V], str):

    @overload # type: ignore
    def __getitem__(self, key: int | slice, /) -> str:
        ...
    @overload
    def __getitem__(self, key: K, /) -> V:
        ...
    def __getitem__(self, key: int | slice | K, /) -> str | V:
        if isinstance(key, (int, slice)):
            return str.__getitem__(self, key)
        else:
            return self.__dict__[key]

    def __iter__(self, /):
        return str.__iter__(self)

    def __len__(self, /):
        return str.__len__(self)


class StrMuMapAttr[K, V](ValueMuMapAttrMixin[K, V], StrMapAttr[K, V], str):
    pass

