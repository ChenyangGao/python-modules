#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 5)
__all__ = [
    "HashObj", "ChunkedHash", "make_accumulator", "file_digest", "file_mdigest", 
    "file_digest_async", "file_mdigest_async", 
]

from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from functools import partial
from hashlib import new as hash_new
from inspect import isasyncgenfunction, isawaitable
from itertools import pairwise
from os import fstat
from typing import overload, runtime_checkable, Any, Literal, Protocol, TypeVar

from asynctools import ensure_async, ensure_aiter
from filewrap import (
    Buffer, SupportsRead, SupportsReadinto, 
    bio_skip_iter, bio_skip_async_iter, 
    bio_chunk_iter, bio_chunk_async_iter, 
    bytes_iter, bytes_async_iter, 
    bytes_iter_skip, bytes_async_iter_skip, 
    bytes_to_chunk_iter, bytes_to_chunk_async_iter, 
)


T = TypeVar("T")


def ensure_slicable_buffer(b: Buffer, /) -> bytes | bytearray | memoryview:
    if isinstance(b, (bytes, bytearray, memoryview)):
        return b
    return memoryview(b)


@runtime_checkable
class HashObj(Protocol):
    def update(self, /, data: bytes):
        pass
    def digest(self, /) -> bytes:
        pass
    def hexdigest(self, /) -> str:
        pass


class ChunkedHash(HashObj):

    def __init__(
        self, 
        /, 
        data: Buffer = b"", 
        alg: str | Callable[[], HashObj] = "md5", 
        block_size: int = 1 << 20, 
        sep: bytes = b"", 
        convert_block: None | Callable[[bytes], bytes] = None, 
    ):
        if isinstance(alg, str):
            alg = partial(hash_new, alg)
        self.factory = alg
        self.block_size = block_size
        self.sep = sep
        self.convert_block = convert_block
        self._block_hash_list: list[bytes] = []
        self._hashobj = alg()
        self._hashobj_cache = alg()
        self._at = 0
        if data:
            self.update(data)

    @property
    def count_read(self, /) -> int:
        return self._at

    def update(self, data: Buffer, /):
        factory = self.factory
        block_size = self.block_size
        block_hash_list = self._block_hash_list
        push_block = block_hash_list.append
        hashobj = self._hashobj
        update = hashobj.update
        hashobj_cache = self._hashobj_cache
        m = ensure_slicable_buffer(data)
        if r := self._at % block_size:
            expect_step = block_size - r
            b = m[:expect_step]
            step = len(b)
            update(b)
            hashobj_cache.update(b)
            self._at += step
            if step < expect_step:
                return
            push_block(hashobj_cache.digest())
            hashobj_cache = self._hashobj_cache = factory()
        for l, r in pairwise(range(r, len(m) + block_size, block_size)):
            b = m[l:r]
            step = len(b)
            update(b)
            hashobj_cache.update(b)
            self._at += step
            if step < block_size:
                return
            push_block(hashobj_cache.digest())
            hashobj_cache = self._hashobj_cache = factory()

    def digest(self, /) -> bytes:
        convert_block = self.convert_block
        if convert_block:
            block_hash_list = list(map(convert_block, self._block_hash_list))
        else:
            block_hash_list = [*self._block_hash_list]
        if self._at % self.block_size:
            hashval = self._hashobj_cache.digest()
            if convert_block:
                hashval = convert_block(hashval)
            block_hash_list.append(hashval)
        hashobj = self.factory()
        update  = hashobj.update
        if sep := self.sep:
            update(block_hash_list[0])
            for bhash in block_hash_list[1:]:
                update(sep)
                update(bhash)
        else:
            for bhash in block_hash_list:
                update(bhash)
        return hashobj.digest()

    def digest_total(self, /) -> bytes:
        return self._hashobj.digest()

    def hexdigest(self, /) -> str:
        return self.digest().hex()

    def hexdigest_total(self, /) -> str:
        return self._hashobj.hexdigest()


@overload
def make_accumulator(
    func: Callable[[Buffer, T], T], 
    /, 
    initial_value: T, 
    async_: Literal[False] = False, 
) -> Callable[[Buffer], T]:
    ...
@overload
def make_accumulator(
    func: Callable[[Buffer, T], T], 
    /, 
    initial_value: T, 
    async_: Literal[True], 
) -> Callable[[Buffer], Awaitable[T]]:
    ...
def make_accumulator(
    func: Callable[[Buffer, T], T], 
    /, 
    initial_value: T, 
    async_: bool = False, 
) -> Callable[[Buffer], T] | Callable[[Buffer], Awaitable[T]]:
    update: Callable[[Buffer], T] | Callable[[Buffer], Awaitable[T]]
    last_value = initial_value
    if isasyncgenfunction(func):
        async def update(value: Buffer, /) -> T:
            nonlocal last_value
            return (last_value := await func(value, last_value)) # type: ignore
    elif async_:
        async def update(value: Buffer, /) -> T:
            nonlocal last_value
            ret = func(value, last_value)
            if isawaitable(ret):
                last_value = await ret
            else:
                last_value = ret
            return last_value
    else:
        def update(value: Buffer, /) -> T:
            nonlocal last_value
            return (last_value := func(value, last_value))
    return update


def file_digest(
    file: Buffer | SupportsRead[Buffer] | SupportsReadinto | Iterable[Buffer], 
    digest: str | HashObj | Callable[[], HashObj] | Callable[[], Callable[[bytes, T], T]] = "md5", 
    /, 
    start: int = 0, 
    stop: None | int = None, 
    chunksize: int = 1 << 16, 
    callback: None | Callable[[int], Any] = None, 
) -> tuple[int, HashObj | T]:
    total, (result,) = file_mdigest(
        file, 
        digest, 
        start=start, 
        stop=stop, 
        chunksize=chunksize, 
        callback=callback, 
    )
    return total, result


def file_mdigest(
    file: Buffer | SupportsRead[Buffer] | SupportsReadinto | Iterable[Buffer], 
    digest: str | HashObj | Callable[[], HashObj] | Callable[[], Callable[[bytes, T], T]] = "md5", 
    /, 
    *digests: str | HashObj | Callable[[], HashObj] | Callable[[], Callable[[bytes, T], T]], 
    start: int = 0, 
    stop: None | int = None, 
    chunksize: int = 1 << 16, 
    callback: None | Callable[[int], Any] = None, 
) -> tuple[int, list[HashObj | T]]:
    def get_digest(alg, /) -> Callable[[bytes], Any]:
        hashobj: HashObj
        if isinstance(alg, str):
            hashobj = hash_new(alg)
        elif isinstance(alg, HashObj):
            hashobj = alg
        else:
            hashfunc = alg()
            if isinstance(hashfunc, HashObj):
                hashobj = hashfunc
            else:
                initial_value = hashfunc(b"")
                results.append(initial_value)
                _update = make_accumulator(
                    hashfunc, 
                    initial_value=initial_value, 
                )
                i = len(results) - 1
                def update(value: bytes, /):
                    ret = results[i] = _update(value)
                    return ret
                return update
        results.append(hashobj)
        return hashobj.update
    results: list = []
    if digests:
        funcs: list[Callable[[bytes], Any]] = []
        funcs.append(get_digest(digest))
        funcs.extend(map(get_digest, digests))
        def update(value: bytes, /):
            for func in funcs:
                func(value)
    else:
        update = get_digest(digest)
    if hasattr(file, "getbuffer"):
        file = file.getbuffer()
    b_it: Iterable[Buffer]
    if isinstance(file, Buffer):
        file = ensure_slicable_buffer(file)[start:stop]
        if not file:
            return 0, results
        if callback is not None:
            callback(start)
        b_it = bytes_to_chunk_iter(file, chunksize=chunksize)
    elif isinstance(file, (SupportsRead, SupportsReadinto)):
        length = -1
        def get_length() -> int:
            nonlocal length
            if length < 0:
                try:
                    fileno = getattr(file, "fileno")()
                    length = fstat(fileno).st_size
                except (AttributeError, OSError):
                    try:
                        length = len(file) # type: ignore
                    except TypeError:
                        raise ValueError(f"can't get file size: {file!r}")
            return length
        if start < 0:
            start += get_length()
        if start < 0:
            start = 0
        if stop is not None:
            if stop < 0:
                stop += get_length()
            if stop <= 0 or start >= stop:
                return 0, results
        if start:
            for _ in bio_skip_iter(file, start, callback=callback):
                pass
        if stop:
            b_it = bio_chunk_iter(file, stop - start, chunksize=chunksize, can_buffer=True)
        else:
            b_it = bio_chunk_iter(file, chunksize=chunksize, can_buffer=True)
    else:
        if start < 0 or stop and stop < 0:
            raise ValueError("negative indices should not be used when using `Iterable[Buffer]`")
        elif stop and start >= stop:
            return 0, results
        b_it = file
        if start:
            b_it = bytes_iter_skip(b_it, start, callback=callback)
        if stop:
            b_it = bytes_iter(b_it, stop - start)
    length = 0
    for chunk in b_it:
        m = ensure_slicable_buffer(chunk)
        update(m)
        length += (size := len(m))
        if callback is not None:
            callback(size)
    return length, results


async def file_digest_async(
    file: Buffer | SupportsRead[Buffer] | SupportsReadinto | Iterable[Buffer] | AsyncIterable[Buffer], 
    digest: str | HashObj | Callable[[], HashObj] | Callable[[], Callable[[bytes, T], T]] | Callable[[], Callable[[bytes, T], Awaitable[T]]] = "md5", 
    /, 
    start: int = 0, 
    stop: None | int = None, 
    chunksize: int = 1 << 16, 
    callback: None | Callable[[int], Any] = None, 
) -> tuple[int, HashObj | T]:
    total, (result,) = await file_mdigest_async(
        file, 
        digest, 
        start=start, 
        stop=stop, 
        chunksize=chunksize, 
        callback=callback, 
    )
    return total, result


async def file_mdigest_async(
    file: Buffer | SupportsRead[Buffer] | SupportsReadinto | Iterable[Buffer] | AsyncIterable[Buffer], 
    digest: str | HashObj | Callable[[], HashObj] | Callable[[], Callable[[bytes, T], T]] | Callable[[], Callable[[bytes, T], Awaitable[T]]] = "md5", 
    /, 
    *digests: str | HashObj | Callable[[], HashObj] | Callable[[], Callable[[bytes, T], T]] | Callable[[], Callable[[bytes, T], Awaitable[T]]], 
    start: int = 0, 
    stop: None | int = None, 
    chunksize: int = 1 << 16, 
    callback: None | Callable[[int], Any] = None, 
) -> tuple[int, list[HashObj | T]]:
    async def get_digest(alg, /) -> Callable[[bytes], Any]:
        hashobj: HashObj
        if isinstance(alg, str):
            hashobj = hash_new(alg)
        elif isinstance(alg, HashObj):
            hashobj = alg
        else:
            hashfunc = alg()
            if isinstance(hashfunc, HashObj):
                hashobj = hashfunc
            else:
                ret = hashfunc(b"")
                if isawaitable(ret):
                    initial_value = await ret
                else:
                    initial_value = ret
                results.append(initial_value)
                _update = make_accumulator(
                    hashfunc, 
                    initial_value=initial_value, 
                    async_=True, 
                )
                i = len(results) - 1
                async def update(value: bytes, /):
                    ret = results[i] = await _update(value)
                    return ret
                return update
        results.append(hashobj)
        return ensure_async(hashobj.update)
    results: list = []
    if digests:
        funcs: list[Callable[[bytes], Any]] = []
        funcs.append(await get_digest(digest))
        for digest in digests:
            funcs.append(await get_digest(digest))
        async def update(value: bytes, /):
            for func in funcs:
                await func(value)
    else:
        update = await get_digest(digest)
    if callback is not None:
        callback = ensure_async(callback)
    if hasattr(file, "getbuffer"):
        file = file.getbuffer()
    b_it: AsyncIterator[Buffer]
    if isinstance(file, Buffer):
        file = ensure_slicable_buffer(file)[start:stop]
        if not file:
            return 0, results
        if callback is not None:
            await callback(start)
        b_it = bytes_to_chunk_async_iter(file, chunksize=chunksize)
    elif isinstance(file, (SupportsRead, SupportsReadinto)):
        length = -1
        def get_length() -> int:
            nonlocal length
            if length < 0:
                try:
                    fileno = getattr(file, "fileno")()
                    length = fstat(fileno).st_size
                except (AttributeError, OSError):
                    try:
                        length = len(file) # type: ignore
                    except TypeError:
                        raise ValueError(f"can't get file size: {file!r}")
            return length
        if start < 0:
            start += get_length()
        if start < 0:
            start = 0
        if stop is not None:
            if stop < 0:
                stop += get_length()
            if stop <= 0 or start >= stop:
                return 0, results
        if start:
            async for _ in bio_skip_async_iter(file, start, callback=callback):
                pass
        if stop:
            b_it = bio_chunk_async_iter(file, stop - start, chunksize=chunksize, can_buffer=True)
        else:
            b_it = bio_chunk_async_iter(file, chunksize=chunksize, can_buffer=True)
    else:
        if start < 0 or stop and stop < 0:
            raise ValueError("negative indices should not be used when using `Iterable[Buffer]`")
        elif stop and start >= stop:
            return 0, results
        b_it = ensure_aiter(file)
        if start:
            b_it = await bytes_async_iter_skip(b_it, start, callback=callback)
        if stop:
            b_it = bytes_async_iter(b_it, stop - start)
    length = 0
    async for chunk in b_it:
        m = ensure_slicable_buffer(chunk)
        await update(m)
        length += (size := len(m))
        if callback is not None:
            await callback(size)
    return length, results

