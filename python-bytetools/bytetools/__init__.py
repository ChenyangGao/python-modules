#!/usr/bin/env python3
# encoding: utf-8

from __future__ import annotations

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__all__ = [
    "is_buffer", "is_mutable_buffer", "zeroblob", 
    "randomblob", "repeatblob", 
]
__version__ = (0, 0, 1)

from os import urandom
from collections.abc import Buffer, Callable
from typing import overload, Literal

from integer_tool import int_to_bytes


def is_buffer(o, /) -> bool:
    return isinstance(o, Buffer)


def is_mutable_buffer(o, /) -> bool:
    if isinstance(o, Buffer):
        try:
            memoryview(o)[:0] = b""
            return True
        except TypeError:
            pass
    return False


@overload
def zeroblob(
    length: int, 
    /, 
    mutable: Literal[False] = False, 
) -> bytes:
    ...
@overload
def zeroblob(
    length: int, 
    /, 
    mutable: Literal[True], 
) -> bytearray:
    ...
def zeroblob(
    length: int, 
    /, 
    mutable: bool = False, 
) -> bytes | bytearray:
    if mutable:
        return bytearray(length)
    return bytes(length)


@overload
def randomblob(
    length: int, 
    /, 
    mutable: Literal[False] = False, 
    urandom: Callable[[int], bytes] = urandom, 
) -> bytes:
    ...
@overload
def randomblob(
    length: int, 
    /, 
    mutable: Literal[True], 
    urandom: Callable[[int], bytes] = urandom, 
) -> bytearray:
    ...
def randomblob(
    length: int, 
    /, 
    mutable: bool = False, 
    urandom: Callable[[int], bytes] = urandom, 
) -> bytes | bytearray:
    if mutable:
        b = bytearray()
        q, r = divmod(length, 1024)
        for _ in range(q):
            b += urandom(1024)
        if r:
            b += urandom(r)
        return b
    return urandom(length)


@overload
def repeatblob(
    length: int, 
    /, 
    mutable: Literal[False] = False, 
    unit: int | Buffer = b"\x00", 
) -> bytes:
    ...
@overload
def repeatblob(
    length: int, 
    /, 
    mutable: Literal[True], 
    unit: int | Buffer = b"\x00", 
) -> bytearray:
    ...
def repeatblob(
    length: int, 
    /, 
    mutable: bool = False, 
    unit: int | Buffer = b"\x00", 
) -> bytes | bytearray:
    def repeat(b, /):
        q, r = divmod(length, len(b))
        suffix = b[:r]
        b *= q
        b += suffix
        return b
    if isinstance(unit, int):
        unit = int_to_bytes(unit)
    if mutable:
        return repeat(bytearray(unit))
    return repeat(bytes(unit))

