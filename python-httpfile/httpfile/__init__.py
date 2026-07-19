#!/usr/bin/env python3
# encoding: utf-8

from __future__ import annotations

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 6)
__all__ = ["HTTPFileReader", "AsyncHTTPFileReader", "MultipartHTTPFileReader", "AsyncMultipartHTTPFileReader"]

from collections.abc import AsyncIterator, Awaitable, Buffer, Callable, Iterable, Iterator
from functools import cached_property
from inspect import isawaitable
from io import (
    BufferedReader, BytesIO, RawIOBase, TextIOWrapper, UnsupportedOperation, 
    DEFAULT_BUFFER_SIZE, 
)
try:
    from shutil import COPY_BUFSIZE # type: ignore
except ImportError:
    COPY_BUFSIZE = 1 << 16
from typing import cast, overload, Any, BinaryIO, Literal, Self
from warnings import warn

from argtools import argcount
from asynctools import ensure_async, run_async
from errno2 import errno
from filewrap import (
    buffer_length, bytes_iter_to_reader, bytes_iter_to_async_reader, 
    to_bytes_view, AsyncBufferedReader, AsyncTextIOWrapper, 
)
from http_request import SupportsGeturl
from http_response import (
    get_filename, get_range, get_status_code, get_total_length, is_range_request, 
)
from yarl import URL


def coalesce(*vals, base=None):
    for v in vals:
        if v is not base:
            return v
    return base


def geturl(url, /) -> str:
    if callable(url):
        url = url()
    if isinstance(url, SupportsGeturl):
        return url.geturl()
    return str(url)


async def ageturl(url, /) -> str:
    if callable(url):
        url = url()
        if isawaitable(url):
            url = await url
    if isinstance(url, SupportsGeturl):
        url = url.geturl()
        if isawaitable(url):
            url = await url
        return url
    return str(url)


class HTTPFileReader[Response](RawIOBase, BinaryIO):

    def __init__(
        self, 
        /, 
        url: (
            str | SupportsGeturl | URL | 
            Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL]
        ), 
        start: int = 0, 
        seek_threshold: int = 1 << 20, 
        request: None | Callable[..., Response] = None, 
        get_file: None | str | Callable[[Response], Any] = None, 
        **request_kwargs, 
    ):
        if start < 0:
            raise ValueError("`start` cannot be < 0")
        self._closed = False
        self._pos = start
        self._url = url
        self.seek_threshold = max(seek_threshold, 0)
        if request is None:
            from urllib3_future_request import request_sync as request # type: ignore
        self.request = cast(Callable[..., Response], request)
        if isinstance(get_file, str):
            if get_file:
                is_method = get_file.endswith("()")
                attrs = get_file.removesuffix("()").split(".")
                def get_file(file, /):
                    for attr in attrs:
                        file = getattr(file, attr)
                    if is_method:
                        file = file()
                    return file
            else:
                get_file = lambda file, /: file
        self.get_file = get_file
        self.request_kwargs = request_kwargs
        headers = request_kwargs["headers"] = dict(request_kwargs.get("headers") or ())
        headers["accept-encoding"] = "identity"

    def __del__(self, /):
        self.close()

    def __enter__(self, /) -> Self:
        return self

    def __exit__(self, /, *_) -> None:
        self.close()

    def __iter__(self, /) -> Self:
        return self

    def __len__(self, /) -> int:
        return self.length

    def __next__(self, /) -> bytes:
        if line := self.readline():
            return line
        raise StopIteration

    def __repr__(self, /) -> str:
        cls = type(self)
        kwargs = {
            "url": self._url, 
            "start": self._pos, 
            "seek_threshold": self.seek_threshold, 
            "request": self.request, 
            "get_file": self.get_file, 
            **self.request_kwargs, 
        }
        return f"{cls.__module__}.{cls.__qualname__}({', '.join(map('%s=%r'.__mod__, kwargs.items()))})"

    @property
    def closed(self, /) -> bool:
        return self._closed

    @property
    def response_closed(self, /) -> bool:
        file = self.__dict__.get("response")
        while file:
            if hasattr(file, "closed"):
                return file.closed
            elif hasattr(file, "is_closed"):
                closed = file.is_closed
                if callable(closed):
                    closed = closed()
                return closed
            file = getattr(file, "raw", None)
        return self.closed

    def _init_info(self, /, key: str = "length"):
        response = self.__dict__.get("response")
        if response is None:
            from urllib3_future_request import request_sync as request
            request_kwargs = dict(self.request_kwargs, parse=...)
            headers = request_kwargs["headers"] = dict(request_kwargs.get("headers") or ())
            headers["range"] = "bytes=0-0"
            response = request(self._geturl(), **request_kwargs)
        self._seekable = not is_range_request(response)
        self.length = coalesce(get_total_length(response), -1)
        match key:
            case "length":
                return self.length
            case "seekable":
                return self._seekable

    @cached_property
    def _seekable(self, /) -> bool:
        return self._init_info("seekable")

    @cached_property
    def length(self, /) -> int:
        return self._init_info("length")

    @cached_property
    def response(self, /):
        self.reconnect()
        return self.__dict__.get("response")

    @property
    def mode(self, /) -> str:
        return "rb"

    @property
    def name(self, /) -> str:
        return get_filename(self.response)

    def _get_file(self, /):
        response = self.__dict__.get("response")
        if not response:
            self.reconnect()
            response = self.__dict__.get("response")
            if not response:
                return None
        if get_file := self.get_file or (getattr(response, "get_file", None)):
            file = get_file(response)
            if hasattr(file, "read"):
                return file
            elif isinstance(file, Iterator):
                return bytes_iter_to_reader(file)
        else:
            if file := getattr(response, "file", None):
                return file
            if hasattr(response, "read"):
                read = response.read
                if argcount(read):
                    return response
            for attr in (
                "iter_bytes", "iter_chunks", "iter_chunked", "iter_stream", 
                "iter_body", "iter_raw", "iter_content", 
            ):
                method = getattr(response, attr, None)
                if method is not None:
                    file = method()
                    if isinstance(file, Iterator):
                        return bytes_iter_to_reader(file)
            for attr in ("stream", "body", "raw", "content"):
                file = getattr(response, attr, None)
                if file is not None:
                    if hasattr(file, "read") and argcount(getattr(file, "read")):
                        return file
                    if callable(file):
                        file = file()
                    if isinstance(file, Buffer):
                        return BytesIO(file)
                    elif isinstance(file, Iterator):
                        return bytes_iter_to_reader(file)
        raise TypeError("can't determine how to `read`")

    def _geturl(self, /) -> str:
        return geturl(self._url)

    def close(self, /):
        if not self.closed:
            if response := self.__dict__.get("response"):
                try:
                    response.close()
                except (AttributeError, TypeError):
                    pass
            self._closed = True

    def fileno(self, /) -> int:
        file = self.__dict__.get("response")
        while file:
            if hasattr(file, "fileno"):
                return file.fileno()
            file = getattr(file, "raw", None)
        return 0

    def flush(self, /):
        if not self.closed:
            file = self.__dict__.get("response")
            while file:
                if hasattr(file, "flush"):
                    return file.flush()
                file = getattr(file, "raw", None)

    def isatty(self, /) -> bool:
        return False

    def _readinto(self, buffer: Buffer, /) -> int:
        read = self.file.read
        m = to_bytes_view(buffer)
        size = len(m)
        start = 0
        last_intv = size - size % COPY_BUFSIZE
        while start < size:
            if start >= last_intv:
                delta = size - start
            else:
                delta = COPY_BUFSIZE
            data = read(delta)
            data_len = len(data)
            m[:data_len] = data
            start += data_len
            if data_len < delta:
                break
        return start

    def _readline(self, size: None | int = -1, /) -> bytes:
        read = self.file.read
        buf = bytearray()
        if size is None or size < 0:
            while b := read(1):
                buf += b
                if b == b"\n":
                    break
        else:
            while size and (b := read(1)):
                buf += b
                if b == b"\n":
                    break
                size -= 1
        return bytes(buf)

    def _readlines(self, hint: int = -1, /) -> list[bytes]:
        try:
            readline = self.file.readline
        except AttributeError:
            readline = self._readline
        ls: list[bytes]
        add = ls.append
        if hint <= 0:
            while l := readline():
                add(l)
        else:
            while hint >= 0 and (l := readline()):
                add(l)
                hint -= len(l)
        return ls

    def readable(self, /) -> bool:
        return True

    def read(self, size: int = -1, /) -> bytes:
        pos = self._pos
        if size == 0:
            return b""
        length = self.__dict__.get("length")
        if length is not None:
            if length == 0 or length > 0 and pos >= length:
                return b""
        if "response" not in self.__dict__ or self.closed or self.response_closed:
            self.reconnect(pos)
        if file := self.file:
            try:
                if size is None or size < 0:
                    data = file.read()
                else:
                    data = file.read(size)
                if data:
                    self._pos = pos + len(data)
                return data
            except:
                self._pos = pos
                self.close()
                raise
        return b""

    def readinto(self, buffer: Buffer, /) -> int:
        pos = self._pos
        if not buffer_length(buffer):
            return 0
        length = self.__dict__.get("length")
        if length is not None:
            if length == 0 or length > 0 and pos >= length:
                return 0
        if "response" not in self.__dict__ or self.closed or self.response_closed:
            self.reconnect(pos)
        if file := self.file:
            try:
                readinto = file.readinto
            except AttributeError:
                readinto = self._readinto
            try:
                size = readinto(buffer)
                if size:
                    self._pos = pos + size
                return size
            except:
                self._pos = pos
                self.close()
                raise
        return 0

    def readline(self, size: None | int = -1, /) -> bytes:
        pos = self._pos
        if size == 0:
            return b""
        length = self.__dict__.get("length")
        if length is not None:
            if length == 0 or length > 0 and pos >= length:
                return b""
        if "response" not in self.__dict__ or self.closed or self.response_closed:
            self.reconnect(pos)
        if file := self.file:
            try:
                readline = file.readline
            except AttributeError:
                readline = self._readline
            try:
                if size is None or size < 0:
                    data = readline()
                else:
                    data = readline(size)
                if data:
                    self._pos = pos + len(data)
                return data
            except:
                self._pos = pos
                self.close()
                raise
        return b""

    def readlines(self, hint: int = -1, /) -> list[bytes]:
        pos = self._pos
        length = self.__dict__.get("length")
        if length is not None:
            if length == 0 or length > 0 and pos >= length:
                return []
        if "response" not in self.__dict__ or self.closed or self.response_closed:
            self.reconnect(pos)
        if file := self.file:
            try:
                readlines = file.readlines
            except AttributeError:
                readlines = self._readlines
            try:
                ls = readlines(hint)
                if ls:
                    self._pos = pos + sum(map(len, ls))
                return ls
            except:
                self._pos = pos
                self.close()
                raise
        return []

    def reconnect(self, /, start: None | int = None) -> int:
        if start is None:
            start = self._pos
        if start and not self.seekable():
            raise OSError(errno.EOPNOTSUPP, "unsupport for reconnection of non-seekable streams")
        request_kwargs = self.request_kwargs
        headers = request_kwargs["headers"] = request_kwargs["headers"]
        if start >= 0:
            headers["range"] = f"bytes={start}-"
        elif start < 0:
            headers["range"] = f"bytes={start}"
        self.close()
        response = self.request(self._geturl(), **request_kwargs)
        status_code = get_status_code(response)
        if not 200 <= status_code < 300:
            raise OSError(
                errno.EIO, 
                {
                    "code": status_code, 
                    "response": response, 
                    "reason": "status code must be in the `range(200, 300)`", 
                }, 
            )
        if start:
            rng = get_range(response)
            if not rng:
                raise OSError(errno.ESPIPE, "non-seekable")
            start = self._pos = rng[0]
        self.response = response
        self.file = self._get_file()
        self._closed = False
        return start

    def seek(self, pos: int, whence: int = 0, /) -> int:
        if not self.seekable():
            raise OSError(errno.EINVAL, "not a seekable stream")
        old_pos = self._pos
        match whence:
            case 1:
                pos = old_pos + pos
            case 2:
                length = self.__dict__.get("length")
                if length is None:
                    length = self._init_info()
                pos += length
        if pos < 0:
            pos = 0
        if pos == old_pos:
            return pos
        if (self.__dict__.get("response") and 
            self.file and 
            not self.closed and 
            not self.response_closed and 
            pos > old_pos and 
            (size := pos - old_pos) <= self.seek_threshold
        ):
            read = self.read
            while size > COPY_BUFSIZE:
                read(COPY_BUFSIZE)
                size -= COPY_BUFSIZE
            read(size)
        return self.reconnect(pos)

    def seekable(self, /) -> bool:
        return self._seekable

    def tell(self, /) -> int:
        return self._pos

    def truncate(self, size: None | int = None, /) -> int:
        raise UnsupportedOperation(errno.ENOTSUP, "truncate")

    def writable(self, /) -> bool:
        return False

    def write(self, b, /) -> int:
        raise UnsupportedOperation(errno.ENOTSUP, "write")

    def writelines(self, lines, /):
        raise UnsupportedOperation(errno.ENOTSUP, "writelines")

    @overload
    @classmethod
    def open(
        cls, 
        /, 
        url: (
            str | SupportsGeturl | URL | 
            Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL]
        ), 
        mode: Literal["br", "rb"], 
        *, 
        buffering: None | int = None, 
        encoding: None | str = None, 
        errors: None | str = None, 
        newline: None | str = None, 
        start: int = 0, 
        seek_threshold: int = 1 << 20, 
        request: None | Callable[..., Response] = None, 
        get_file: None | str | Callable[[Response], Any] = None, 
        **request_kwargs, 
    ) -> Self | BufferedReader:
        ...
    @overload
    @classmethod
    def open(
        cls, 
        /, 
        url: (
            str | SupportsGeturl | URL | 
            Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL]
        ), 
        mode: Literal["r", "rt", "tr"] = "r", 
        *, 
        buffering: None | int = None, 
        encoding: None | str = None, 
        errors: None | str = None, 
        newline: None | str = None, 
        start: int = 0, 
        seek_threshold: int = 1 << 20, 
        request: None | Callable[..., Response] = None, 
        get_file: None | str | Callable[[Response], Any] = None, 
        **request_kwargs, 
    ) -> TextIOWrapper:
        ...
    @classmethod
    def open(
        cls, 
        /, 
        url: (
            str | SupportsGeturl | URL | 
            Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL]
        ), 
        mode: Literal["r", "rt", "tr", "br", "rb"] = "r", 
        *, 
        buffering: None | int = None, 
        encoding: None | str = None, 
        errors: None | str = None, 
        newline: None | str = None, 
        start: int = 0, 
        seek_threshold: int = 1 << 20, 
        request: None | Callable[..., Response] = None, 
        get_file: None | str | Callable[[Response], Any] = None, 
        **request_kwargs, 
    ) -> Self | BufferedReader | TextIOWrapper:
        file = cls(
            url=url, 
            start=start, 
            seek_threshold=seek_threshold, 
            request=request, 
            get_file=get_file, 
            **request_kwargs, 
        )
        if mode not in ("r", "rt", "tr", "rb", "br"):
            raise OSError(errno.EINVAL, f"invalid (or unsupported) mode: {mode!r}")
        return file.wrap(
            text_mode="b" not in cast(str, mode), # type: ignore
            buffering=buffering, 
            encoding=encoding, 
            errors=errors, 
            newline=newline, 
        )

    @overload
    def wrap(
        self, 
        /, 
        text_mode: Literal[False] = False, 
        *, 
        buffering: None | int = None, 
        encoding: None | str = None, 
        errors: None | str = None, 
        newline: None | str = None, 
    ) -> Self | BufferedReader:
        ...
    @overload
    def wrap(
        self, 
        /, 
        text_mode: Literal[True], 
        *, 
        buffering: None | int = None, 
        encoding: None | str = None, 
        errors: None | str = None, 
        newline: None | str = None, 
    ) -> TextIOWrapper:
        ...
    def wrap(
        self, 
        /, 
        text_mode: Literal[False, True] = False, 
        *, 
        buffering: None | int = None, 
        encoding: None | str = None, 
        errors: None | str = None, 
        newline: None | str = None, 
    ) -> Self | BufferedReader | TextIOWrapper:
        if buffering is None:
            if text_mode:
                buffering = DEFAULT_BUFFER_SIZE
            else:
                buffering = 0
        if buffering == 0:
            if text_mode:
                raise OSError(errno.EINVAL, "can't have unbuffered text I/O")
            return self
        line_buffering = False
        buffer_size: int
        if buffering < 0:
            buffer_size = DEFAULT_BUFFER_SIZE
        elif buffering == 1:
            if not text_mode:
                warn("line buffering (buffering=1) isn't supported in binary mode, "
                     "the default buffer size will be used", RuntimeWarning)
            buffer_size = DEFAULT_BUFFER_SIZE
            line_buffering = True
        else:
            buffer_size = buffering
        if "response" not in self.__dict__ or self.closed or self.response_closed:
            self.reconnect()
        raw = self
        buffer: BufferedReader = BufferedReader(raw, buffer_size)
        if text_mode:
            return TextIOWrapper(
                buffer, 
                encoding=encoding, 
                errors=errors, 
                newline=newline, 
                line_buffering=line_buffering, 
            )
        else:
            return buffer


class AsyncHTTPFileReader[Response](RawIOBase, BinaryIO):

    def __init__(
        self, 
        /, 
        url: (
            str | SupportsGeturl | URL | 
            Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL] | 
            Callable[[], Awaitable[str]] | Callable[[], Awaitable[SupportsGeturl]] | Callable[[], Awaitable[URL]]
        ), 
        start: int = 0, 
        seek_threshold: int = 1 << 20, 
        request: None | Callable[..., Awaitable[Response]] = None, 
        get_file: None | str | Callable[[Response], Any] = None, 
        **request_kwargs, 
    ):
        if start < 0:
            raise ValueError("`start` cannot be < 0")
        self._closed = False
        self._pos = start
        self._url = url
        self.seek_threshold = max(seek_threshold, 0)
        if request is None:
            from urllib3_future_request import request_async as request # type: ignore
        self.request = cast(Callable[..., Awaitable[Response]], request)
        if isinstance(get_file, str):
            if get_file:
                is_method = get_file.endswith("()")
                attrs = get_file.removesuffix("()").split(".")
                def get_file(file, /):
                    for attr in attrs:
                        file = getattr(file, attr)
                    if is_method:
                        file = file()
                    return file
            else:
                get_file = lambda file, /: file
        self.get_file = get_file
        self.request_kwargs = request_kwargs
        headers = request_kwargs["headers"] = dict(request_kwargs.get("headers") or ())
        headers["accept-encoding"] = "identity"

    @classmethod
    async def new(
        cls, 
        /, 
        url: (
            str | SupportsGeturl | URL | 
            Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL] | 
            Callable[[], Awaitable[str]] | Callable[[], Awaitable[SupportsGeturl]] | Callable[[], Awaitable[URL]]
        ), 
        start: int = 0, 
        seek_threshold: int = 1 << 20, 
        request: None | Callable[..., Awaitable[Response]] = None, 
        get_file: None | str | Callable[[Response], Any] = None, 
        **request_kwargs, 
    ) -> Self:
        self = cls(
            url, 
            start=start, 
            seek_threshold=seek_threshold, 
            request=request, 
            get_file=get_file, 
            **request_kwargs, 
        )
        await self.reconnect()
        return self

    def __del__(self, /):
        self.close()

    async def __aenter__(self, /) -> Self:
        return self

    async def __aexit__(self, /, *_):
        await self.aclose()

    def __aiter__(self, /) -> Self:
        return self

    async def __anext__(self, /) -> bytes:
        if line := await self.readline():
            return line
        raise StopAsyncIteration

    def __len__(self, /) -> int:
        return self.length

    def __repr__(self, /) -> str:
        cls = type(self)
        kwargs = {
            "url": self._url, 
            "start": self._pos, 
            "seek_threshold": self.seek_threshold, 
            "request": self.request, 
            "get_file": self.get_file, 
            **self.request_kwargs, 
        }
        return f"{cls.__module__}.{cls.__qualname__}({', '.join(map('%s=%r'.__mod__, kwargs.items()))})"

    @property
    def closed(self, /) -> bool:
        return self._closed

    @property
    def response_closed(self, /) -> bool:
        file = self.__dict__.get("response")
        while file:
            if hasattr(file, "closed"):
                return file.closed
            elif hasattr(file, "is_closed"):
                closed = file.is_closed
                if callable(closed):
                    closed = closed()
                return closed
            file = getattr(file, "raw", None)
        return self.closed

    async def _init_info(self, /, key: str = "length"):
        response = self.__dict__.get("response")
        if response is None:
            from urllib3_future_request import request_async as request
            request_kwargs = dict(self.request_kwargs, parse=...)
            headers = request_kwargs["headers"] = dict(request_kwargs.get("headers") or ())
            headers["range"] = "bytes=0-0"
            response = await request(await self._geturl(), **request_kwargs)
        self._seekable = not is_range_request(response)
        self.length = coalesce(get_total_length(response), -1)
        match key:
            case "length":
                return self.length
            case "seekable":
                return self._seekable

    @cached_property
    def _seekable(self, /) -> bool:
        return run_async(self._init_info("seekable"))

    @cached_property
    def length(self, /) -> int:
        return run_async(self._init_info("length"))

    @cached_property
    def response(self, /):
        run_async(self.reconnect())
        return self.__dict__.get("response")

    @property
    def mode(self, /) -> str:
        return "rb"

    @property
    def name(self, /) -> str:
        return get_filename(self.response)

    async def _get_file(self, /):
        response = self.__dict__.get("response")
        if not response:
            await self.reconnect()
            response = self.__dict__.get("response")
            if not response:
                return None
        if get_file := self.get_file or (getattr(response, "get_file", None)):
            file = get_file(response)
            if isawaitable(file):
                file = await file
            if hasattr(file, "aread") or hasattr(file, "read"):
                return file
            elif isinstance(file, (Iterator, AsyncIterator)):
                return bytes_iter_to_async_reader(file, threaded=True)
        else:
            if file := getattr(response, "file", None):
                return file
            for attr in ("aread", "read"):
                method = getattr(response, attr, None)
                if method is not None and argcount(method):
                    return response
            for attr in (
                "aiter_bytes", "aiter_chunks", "aiter_chunked", "aiter_stream", 
                "aiter_body", "aiter_raw", "aiter_content", 
                "iter_bytes", "iter_chunks", "iter_chunked", "iter_stream", 
                "iter_body", "iter_raw", "iter_content", 
            ):
                method = getattr(response, attr, None)
                if method is not None:
                    file = method()
                    if isinstance(file, (Iterator, AsyncIterator)):
                        return bytes_iter_to_async_reader(file, threaded=True)
            for attr in (
                "astream", "abody", "araw", "acontent", 
                "stream", "body", "raw", "content", 
            ):
                file = getattr(response, attr, None)
                if file is not None:
                    if isawaitable(file):
                        file = await file
                    for attr in ("aread", "read"):
                        method = getattr(file, attr, None)
                        if method is not None and argcount(method):
                            return file
                    if callable(file):
                        file = file()
                        if isawaitable(file):
                            file = await file
                    if isinstance(file, Buffer):
                        return BytesIO(file)
                    elif isinstance(file, (Iterator, AsyncIterator)):
                        return bytes_iter_to_async_reader(file, threaded=True)
        raise TypeError("can't determine how to `read`")

    async def _geturl(self, /) -> str:
        return await ageturl(self._url)

    async def close_response(self, /):
        if response := self.__dict__.get("response"):
            try:
                ret = response.aclose()
            except (AttributeError, TypeError):
                try:
                    ret = response.close()
                except (AttributeError, TypeError):
                    return
            if isawaitable(ret):
                await ret

    async def aclose(self, /):
        if not self.closed:
            await self.close_response()
            self._closed = True

    def close(self, /):
        from asynctools import run_async
        run_async(self.aclose())

    def fileno(self, /) -> int:
        file = self.__dict__.get("response")
        while file:
            if hasattr(file, "fileno"):
                return file.fileno()
            file = getattr(file, "raw", None)
        return 0

    async def flush(self, /):
        if not self.closed:
            file = self.__dict__.get("response")
            while file:
                if hasattr(file, "flush"):
                    ret = file.flush()
                    if isawaitable(ret):
                        ret = await ret
                    return ret
                file = getattr(file, "raw", None)

    def isatty(self, /) -> bool:
        return False

    def _get_read(self, /):
        try:
            return self.file.aread
        except AttributeError:
            return ensure_async(self.file.read, threaded=True)

    async def _readinto(self, buffer: Buffer, /) -> int:
        read = self._get_read()
        m = to_bytes_view(buffer)
        size = len(m)
        start = 0
        last_intv = size - size % COPY_BUFSIZE
        while start < size:
            if start >= last_intv:
                delta = size - start
            else:
                delta = COPY_BUFSIZE
            data = await read(delta)
            data_len = len(data)
            m[:data_len] = data
            start += data_len
            if data_len < delta:
                break
        return start

    async def _readline(self, size: None | int = -1, /) -> bytes:
        read = self._get_read()
        buf = bytearray()
        if size is None or size < 0:
            while b := await read(1):
                buf += b
                if b == b"\n":
                    break
        else:
            while size and (b := await read(1)):
                buf += b
                if b == b"\n":
                    break
                size -= 1
        return bytes(buf)

    async def _readlines(self, hint: int = -1, /) -> list[bytes]:
        try:
            readline = self.file.areadline
        except AttributeError:
            try:
                readline = ensure_async(self.file.readline, threaded=True)
            except AttributeError:
                readline = self._readline
        ls: list[bytes]
        add = ls.append
        if hint <= 0:
            while l := await readline():
                add(l)
        else:
            while hint >= 0 and (l := await readline()):
                add(l)
                hint -= len(l)
        return ls

    def readable(self, /) -> bool:
        return True

    async def read(self, size: int = -1, /) -> bytes: # type: ignore
        pos = self._pos
        if size == 0:
            return b""
        length = self.__dict__.get("length")
        if length is not None:
            if length == 0 or length > 0 and pos >= length:
                return b""
        if "response" not in self.__dict__ or self.closed or self.response_closed:
            await self.reconnect(pos)
        if self.file:
            read = self._get_read()
            try:
                if size is None or size < 0:
                    data = await read()
                else:
                    data = await read(size)
                if data:
                    self._pos = pos + len(data)
                return data
            except:
                self._pos = pos
                await self.aclose()
                raise
        return b""

    async def readinto(self, buffer, /) -> int: # type: ignore
        pos = self._pos
        if not buffer_length(buffer):
            return 0
        length = self.__dict__.get("length")
        if length is not None:
            if length == 0 or length > 0 and pos >= length:
                return 0
        if "response" not in self.__dict__ or self.closed or self.response_closed:
            await self.reconnect(pos)
        if file := self.file:
            try:
                readinto = file.areadinto
            except AttributeError:
                try:
                    readinto = ensure_async(file.readinto, threaded=True)
                except AttributeError:
                    readinto = self._readinto
            try:
                size = await readinto(buffer)
                if size:
                    self._pos = pos + size
                return size
            except:
                self._pos = pos
                await self.aclose()
                raise
        return 0

    async def readline(self, size: None | int = -1, /) -> bytes: # type: ignore
        pos = self._pos
        if size == 0:
            return b""
        length = self.__dict__.get("length")
        if length is not None:
            if length == 0 or length > 0 and pos >= length:
                return b""
        if "response" not in self.__dict__ or self.closed or self.response_closed:
            await self.reconnect(pos)
        if file := self.file:
            try:
                readline = file.areadline
            except AttributeError:
                try:
                    readline = ensure_async(file.readline, threaded=True)
                except AttributeError:
                    readline = self._readline
            try:
                if size is None or size < 0:
                    data = await readline()
                else:
                    data = await readline(size)
                if data:
                    self._pos = pos + len(data)
                return data
            except:
                self._pos = pos
                await self.aclose()
                raise
        return b""

    async def readlines(self, hint: int = -1, /) -> list[bytes]: # type: ignore
        pos = self._pos
        length = self.__dict__.get("length")
        if length is not None:
            if length == 0 or length > 0 and pos >= length:
                return []
        if "response" not in self.__dict__ or self.closed or self.response_closed:
            await self.reconnect(pos)
        if file := self.file:
            try:
                readlines = file.areadlines
            except AttributeError:
                try:
                    readlines = ensure_async(file.readlines, threaded=True)
                except AttributeError:
                    readlines = self._readlines
            try:
                ls = await readlines(hint)
                if ls:
                    self._pos = pos + sum(map(len, ls))
                return ls
            except:
                self._pos = pos
                await self.aclose()
                raise
        return []

    async def reconnect(self, /, start: None | int = None) -> int:
        if start is None:
            start = self._pos
        if start and not self.seekable():
            raise OSError(errno.EOPNOTSUPP, "unsupport for reconnection of non-seekable streams")
        request_kwargs = self.request_kwargs
        headers = request_kwargs["headers"] = request_kwargs["headers"]
        if start >= 0:
            headers["range"] = f"bytes={start}-"
        elif start < 0:
            headers["range"] = f"bytes={start}"
        await self.aclose()
        response = await self.request(await self._geturl(), **request_kwargs)
        status_code = get_status_code(response)
        if not 200 <= status_code < 300:
            raise OSError(
                errno.EIO, 
                {
                    "code": status_code, 
                    "response": response, 
                    "reason": "status code must be in the `range(200, 300)`", 
                }, 
            )
        if start:
            rng = get_range(response)
            if not rng:
                raise OSError(errno.ESPIPE, "non-seekable")
            start = self._pos = rng[0]
        self.response = response
        self.file = await self._get_file()
        self._closed = False
        return start

    async def seek(self, pos: int, whence: int = 0, /) -> int: # type: ignore
        if not self.seekable():
            raise OSError(errno.EINVAL, "not a seekable stream")
        old_pos = self._pos
        match whence:
            case 1:
                pos = old_pos + pos
            case 2:
                length = self.__dict__.get("length")
                if length is None:
                    length = await self._init_info()
                pos += length
        if pos < 0:
            pos = 0
        if (self.__dict__.get("response") and 
            self.file and 
            not self.closed and 
            not self.response_closed and 
            pos > old_pos and 
            (size := pos - old_pos) <= self.seek_threshold
        ):
            read = self._get_read()
            while size > COPY_BUFSIZE:
                await read(COPY_BUFSIZE)
                size -= COPY_BUFSIZE
            await read(size)
        return await self.reconnect(pos)

    def seekable(self, /) -> bool:
        return self._seekable

    def tell(self, /) -> int:
        return self._pos

    async def truncate(self, size: None | int = None, /) -> int: # type: ignore
        raise UnsupportedOperation(errno.ENOTSUP, "truncate")

    async def writable(self, /) -> bool: # type: ignore
        return False

    async def write(self, b, /) -> int: # type: ignore
        raise UnsupportedOperation(errno.ENOTSUP, "write")

    async def writelines(self, lines, /): # type: ignore
        raise UnsupportedOperation(errno.ENOTSUP, "writelines")

    @overload
    @classmethod
    async def open(
        cls, 
        /, 
        url: (
            str | SupportsGeturl | URL | 
            Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL] | 
            Callable[[], Awaitable[str]] | Callable[[], Awaitable[SupportsGeturl]] | Callable[[], Awaitable[URL]]
        ), 
        mode: Literal["br", "rb"], 
        *, 
        buffering: None | int = None, 
        encoding: None | str = None, 
        errors: None | str = None, 
        newline: None | str = None, 
        start: int = 0, 
        seek_threshold: int = 1 << 20, 
        request: None | Callable[..., Awaitable[Response]] = None, 
        get_file: None | str | Callable[[Response], Any] = None, 
        **request_kwargs, 
    ) -> Self | AsyncBufferedReader:
        ...
    @overload
    @classmethod
    async def open(
        cls, 
        /, 
        url: (
            str | SupportsGeturl | URL | 
            Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL] | 
            Callable[[], Awaitable[str]] | Callable[[], Awaitable[SupportsGeturl]] | Callable[[], Awaitable[URL]]
        ), 
        mode: Literal["r", "rt", "tr"] = "r", 
        *, 
        buffering: None | int = None, 
        encoding: None | str = None, 
        errors: None | str = None, 
        newline: None | str = None, 
        start: int = 0, 
        seek_threshold: int = 1 << 20, 
        request: None | Callable[..., Awaitable[Response]] = None, 
        get_file: None | str | Callable[[Response], Any] = None, 
        **request_kwargs, 
    ) -> AsyncTextIOWrapper:
        ...
    @classmethod
    async def open(
        cls, 
        /, 
        url: (
            str | SupportsGeturl | URL | 
            Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL] | 
            Callable[[], Awaitable[str]] | Callable[[], Awaitable[SupportsGeturl]] | Callable[[], Awaitable[URL]]
        ), 
        mode: Literal["r", "rt", "tr", "br", "rb"] = "r", 
        *, 
        buffering: None | int = None, 
        encoding: None | str = None, 
        errors: None | str = None, 
        newline: None | str = None, 
        start: int = 0, 
        seek_threshold: int = 1 << 20, 
        request: None | Callable[..., Awaitable[Response]] = None, 
        get_file: None | str | Callable[[Response], Any] = None, 
        **request_kwargs, 
    ) -> Self | AsyncBufferedReader | AsyncTextIOWrapper:
        file = cls(
            url, 
            start=start, 
            seek_threshold=seek_threshold, 
            request=request, 
            get_file=get_file, 
            **request_kwargs, 
        )
        if mode not in ("r", "rt", "tr", "rb", "br"):
            raise OSError(errno.EINVAL, f"invalid (or unsupported) mode: {mode!r}")
        return file.wrap(
            text_mode="b" not in cast(str, mode), # type: ignore
            buffering=buffering, 
            encoding=encoding, 
            errors=errors, 
            newline=newline, 
        )

    @overload
    async def wrap(
        self, 
        /, 
        text_mode: Literal[False] = False, 
        *, 
        buffering: None | int = None, 
        encoding: None | str = None, 
        errors: None | str = None, 
        newline: None | str = None, 
    ) -> Self | AsyncBufferedReader:
        ...
    @overload
    async def wrap(
        self, 
        /, 
        text_mode: Literal[True], 
        *, 
        buffering: None | int = None, 
        encoding: None | str = None, 
        errors: None | str = None, 
        newline: None | str = None, 
    ) -> AsyncTextIOWrapper:
        ...
    async def wrap(
        self, 
        /, 
        text_mode: bool = False, 
        *, 
        buffering: None | int = None, 
        encoding: None | str = None, 
        errors: None | str = None, 
        newline: None | str = None, 
    ) -> Self | AsyncBufferedReader | AsyncTextIOWrapper:
        if buffering is None:
            if text_mode:
                buffering = DEFAULT_BUFFER_SIZE
            else:
                buffering = 0
        if buffering == 0:
            if text_mode:
                raise OSError(errno.EINVAL, "can't have unbuffered text I/O")
            return self
        line_buffering = False
        buffer_size: int
        if buffering < 0:
            buffer_size = DEFAULT_BUFFER_SIZE
        elif buffering == 1:
            if not text_mode:
                warn("line buffering (buffering=1) isn't supported in binary mode, "
                     "the default buffer size will be used", RuntimeWarning)
            buffer_size = DEFAULT_BUFFER_SIZE
            line_buffering = True
        else:
            buffer_size = buffering
        if "response" not in self.__dict__ or self.closed or self.response_closed:
            await self.reconnect()
        raw = self
        buffer = AsyncBufferedReader(raw, buffer_size)
        if text_mode:
            return AsyncTextIOWrapper(
                buffer, 
                encoding=encoding, 
                errors=errors, 
                newline=newline, 
                line_buffering=line_buffering, 
            )
        else:
            return buffer


class MultipartHTTPFileReader[Response](HTTPFileReader[Response]):

    def __init__(
        self, 
        /, 
        urls: (
            str | SupportsGeturl | URL | 
            Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL] | 
            tuple[int, str] | tuple[int, SupportsGeturl] | tuple[int, URL] | 
            tuple[int, Callable[[], str]] | tuple[int, Callable[[], SupportsGeturl]] | tuple[int, Callable[[], URL]] |
            Iterable[
                str | SupportsGeturl | URL | 
                Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL] | 
                tuple[int, str] | tuple[int, SupportsGeturl] | tuple[int, URL] | 
                tuple[int, Callable[[], str]] | tuple[int, Callable[[], SupportsGeturl]] | tuple[int, Callable[[], URL]]
            ]
        ), 
        start: int = 0, 
        seek_threshold: int = 1 << 20, 
        request: None | Callable[..., Response] = None, 
        get_file: None | str | Callable[[Response], Any] = None, 
        **request_kwargs, 
    ):
        self.seek_threshold = seek_threshold
        if request is None:
            from urllib3_future_request import request_sync as request # type: ignore
        self.request = cast(Callable[..., Response], request)
        self.get_file = get_file # type: ignore
        self.request_kwargs = request_kwargs
        if (isinstance(urls, (str, SupportsGeturl, URL)) or 
            callable(urls) or 
            isinstance(urls, tuple) and urls and isinstance(urls[0], int)
        ):
            urls = [urls]
        else:
            urls = list(urls) # type: ignore

        from urllib3_future_request import request_sync

        self._parts: list[dict] = []
        add_part = self._parts.append
        seekable = True
        length = 0
        request_kwargs = dict(request_kwargs, parse=...)
        headers = request_kwargs["headers"] = dict(request_kwargs.get("headers") or ())
        headers["range"] = "bytes=0-0"
        for url in urls:
            if isinstance(url, tuple):
                size, url = url
            else:
                url_ = url
                if callable(url_):
                    url_ = url_()
                if isinstance(url_, SupportsGeturl):
                    url_ = url_.geturl()
                else:
                    url_ = str(url_)
                resp = request_sync(url_, **request_kwargs)
                if not is_range_request(resp):
                    seekable = False
                size = get_total_length(resp) or 0
            if size:
                start = length
                length += size
                stop = length
                add_part({
                    "url": url, 
                    "length": size, 
                    "start": start, 
                    "stop": stop, 
                })
        self.length = length
        if start < 0:
            start += length
            if start < 0:
                start = 0
        self._pos = start
        self._seekable = seekable
        self._closed = False

    def __repr__(self):
        cls = type(self)
        kwargs = {
            "urls": [(d["length"], d["url"]) for d in self._parts], 
            "start": self._pos, 
            "seek_threshold": self.seek_threshold, 
            "request": self.request, 
            "get_file": self.get_file, 
            **self.request_kwargs, 
        }
        return f"{cls.__module__}.{cls.__qualname__}({', '.join(map('%s=%r'.__mod__, kwargs.items()))})"

    @cached_property
    def response(self, /):
        return self.file.response

    def close(self, /):
        self.file.close()
        self._closed = True

    def read(self, size: None | int = -1, /) -> bytes:
        if size is None or size < 0:
            size = self.length - self._pos
        else:
            size = min(self.length - self._pos, size)
        if size <= 0:
            return b""
        buf = bytearray(size)
        self.readinto(buf)
        return bytes(buf)

    def readinto(self, buffer: Buffer, /) -> int:
        pos = self._pos
        remaining = min(self.length - pos, buffer_length(buffer))
        if remaining <= 0:
            return 0
        if "file" not in self.__dict__ or self.closed or self.response_closed:
            self.reconnect(pos)
        try:
            readinto = self.file.readinto
            view = to_bytes_view(buffer)[:remaining]
            size = 0
            while size < remaining:
                start_pos = pos + size
                self.seek(start_pos)
                size += readinto(view[size:])
            return size
        except:
            self._pos = pos
            self.close()
            raise

    def readline(self, size: None | int = -1, /) -> bytes:
        pos = self._pos
        if size is None or size < 0:
            size = self.length - pos
        else:
            size = min(self.length - pos, size)
        if size <= 0:
            return b""
        if "file" not in self.__dict__ or self.closed or self.response_closed:
            self.reconnect(pos)
        try:
            readline = self.file.readline
            buf = bytearray()
            while size > 0:
                if line := readline(size):
                    buf += line
                    if buf.endswith(b"\n"):
                        break
                else:
                    self.seek(pos + len(buf))
                size -= len(line)
            return bytes(buf)
        except:
            self._pos = pos
            self.close()
            raise

    def readlines(self, hint: int = -1, /) -> list[bytes]:
        pos = self._pos
        readline = self.readline
        ls: list[bytes]
        add = ls.append
        try:
            if hint <= 0:
                while l := readline():
                    add(l)
            else:
                while hint >=0. and (l := readline()):
                    add(l)
                    hint -= len(l)
            return ls
        except:
            self._pos = pos
            self.close()
            raise

    def read_range(self, size: None | int = -1, /, offset: int = 0) -> bytes:
        if offset < 0:
            offset += self.length
            if offset < 0:
                offset = 0
        if size is None or size < 0:
            size = self.length - offset
        else:
            size = min(self.length - offset, size)
        if size <= 0:
            return b""
        buf = bytearray(size)
        self.readinto_range(buf, offset)
        return bytes(buf)

    def readinto_range(self, buffer: Buffer, /, offset: int = 0):
        from urllib3_future_request import request_sync as request

        if offset < 0:
            offset += self.length
            if offset < 0:
                offset = 0
        remaining = min(self.length - offset, buffer_length(buffer))
        if remaining <= 0:
            return 0
        request_kwargs = dict(self.request_kwargs)
        headers = request_kwargs["headers"] = dict(request_kwargs.get("headers") or ())
        headers["accept-encoding"] = "identity"
        view = to_bytes_view(buffer)[:remaining]
        size = 0
        while size < remaining:
            for vol in self._parts:
                if vol["start"] <= offset < vol["stop"]:
                    vol_offset = offset + size - vol["start"]
                    headers["range"] = f"bytes={vol_offset}-"
                    resp = request(vol["url"], **request_kwargs)
                    try:
                        if vol_offset and resp.headers.get("accept-ranges") != "bytes":
                            raise OSError(29, "non-seekable stream")
                        size += resp.readinto(view[size:])
                    finally:
                        resp.close()
        return size

    def reconnect(self, /, start: None | int = None) -> int:
        if start is None:
            start = self._pos
        elif start < 0:
            start += self.length
            if start < 0:
                start = 0
        if start >= self.length:
            return start
        for vol in self._parts:
            if vol["start"] <= start < vol["stop"]:
                self.close()
                file = self.file = HTTPFileReader(
                    vol["url"], 
                    start=vol["start"] - start, 
                    seek_threshold=self.seek_threshold, 
                    request=self.request, 
                    get_file=self.get_file, 
                    **self.request_kwargs, 
                )
                self.response = file.response
                self._pos = start
                self._closed = False
                return start
        return start

    def seek(self, pos: int, whence: int = 0, /) -> int:
        old_pos = self._pos
        match whence:
            case 1:
                pos = old_pos + pos
            case 2:
                pos += self.length
        if pos < 0:
            pos = 0
        if pos == old_pos:
            return pos
        elif pos >= self.length:
            self.close()
            self._pos = pos
            return pos
        if hasattr(self, "file"):
            file = self.file
            start = old_pos - file._pos
            stop = start + file.length
            if start <= pos < stop:
                file.seek(pos-start)
                self._pos = pos
                return pos
        return self.reconnect(pos)


class AsyncMultipartHTTPFileReader[Response](AsyncHTTPFileReader[Response]):

    def __init__(
        self, 
        /, 
        urls: (
            str | SupportsGeturl | URL | 
            Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL] | 
            Callable[[], Awaitable[str]] | Callable[[], Awaitable[SupportsGeturl]] | Callable[[], Awaitable[URL]] | 
            tuple[int, str] | tuple[int, SupportsGeturl] | tuple[int, URL] | 
            tuple[int, Callable[[], str]] | tuple[int, Callable[[], SupportsGeturl]] | tuple[int, Callable[[], URL]] | 
            tuple[int, Callable[[], Awaitable[str]]] | tuple[int, Callable[[], Awaitable[SupportsGeturl]]] | tuple[int, Callable[[], Awaitable[URL]]] | 
            Iterable[
                str | SupportsGeturl | URL | 
                Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL] | 
                Callable[[], Awaitable[str]] | Callable[[], Awaitable[SupportsGeturl]] | Callable[[], Awaitable[URL]] | 
                tuple[int, str] | tuple[int, SupportsGeturl] | tuple[int, URL] | 
                tuple[int, Callable[[], str]] | tuple[int, Callable[[], SupportsGeturl]] | tuple[int, Callable[[], URL]] | 
                tuple[int, Callable[[], Awaitable[str]]] | tuple[int, Callable[[], Awaitable[SupportsGeturl]]] | tuple[int, Callable[[], Awaitable[URL]]]
            ]
        ), 
        start: int = 0, 
        seek_threshold: int = 1 << 20, 
        request: None | Callable[..., Awaitable[Response]] = None, 
        get_file: None | str | Callable[[Response], Any] = None, 
        **request_kwargs, 
    ):
        run_async(self.__ainit__(
            urls, 
            start=start, 
            seek_threshold=seek_threshold, 
            request=request, 
            get_file=get_file, 
            **request_kwargs, 
        ))

    async def __ainit__(
        self, 
        /, 
        urls: (
            str | SupportsGeturl | URL | 
            Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL] | 
            Callable[[], Awaitable[str]] | Callable[[], Awaitable[SupportsGeturl]] | Callable[[], Awaitable[URL]] | 
            tuple[int, str] | tuple[int, SupportsGeturl] | tuple[int, URL] | 
            tuple[int, Callable[[], str]] | tuple[int, Callable[[], SupportsGeturl]] | tuple[int, Callable[[], URL]] | 
            tuple[int, Callable[[], Awaitable[str]]] | tuple[int, Callable[[], Awaitable[SupportsGeturl]]] | tuple[int, Callable[[], Awaitable[URL]]] | 
            Iterable[
                str | SupportsGeturl | URL | 
                Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL] | 
                Callable[[], Awaitable[str]] | Callable[[], Awaitable[SupportsGeturl]] | Callable[[], Awaitable[URL]] | 
                tuple[int, str] | tuple[int, SupportsGeturl] | tuple[int, URL] | 
                tuple[int, Callable[[], str]] | tuple[int, Callable[[], SupportsGeturl]] | tuple[int, Callable[[], URL]] | 
                tuple[int, Callable[[], Awaitable[str]]] | tuple[int, Callable[[], Awaitable[SupportsGeturl]]] | tuple[int, Callable[[], Awaitable[URL]]]
            ]
        ), 
        start: int = 0, 
        seek_threshold: int = 1 << 20, 
        request: None | Callable[..., Awaitable[Response]] = None, 
        get_file: None | str | Callable[[Response], Any] = None, 
        **request_kwargs, 
    ):
        self.seek_threshold = seek_threshold
        if request is None:
            from urllib3_future_request import request_async as request # type: ignore
        self.request = cast(Callable[..., Awaitable[Response]], request)
        self.get_file = get_file # type: ignore
        self.request_kwargs = request_kwargs
        if (isinstance(urls, (str, SupportsGeturl, URL)) or 
            callable(urls) or 
            isinstance(urls, tuple) and urls and isinstance(urls[0], int)
        ):
            urls = [urls]
        else:
            urls = list(urls) # type: ignore

        from urllib3_future_request import request_async

        self._parts: list[dict] = []
        add_part = self._parts.append
        seekable = True
        length = 0
        request_kwargs = dict(request_kwargs, parse=...)
        headers = request_kwargs["headers"] = dict(request_kwargs.get("headers") or ())
        headers["range"] = "bytes=0-0"
        for url in urls:
            if isinstance(url, tuple):
                size, url = url
            else:
                resp = await request_async(await ageturl(url), **request_kwargs)
                if not is_range_request(resp):
                    seekable = False
                size = get_total_length(resp) or 0
            if size:
                start = length
                length += size
                stop = length
                add_part({
                    "url": url, 
                    "length": size, 
                    "start": start, 
                    "stop": stop, 
                })
        self.length = length
        if start < 0:
            start += length
            if start < 0:
                start = 0
        self._pos = start
        self._seekable = seekable
        self._closed = False

    @classmethod
    async def new( # type: ignore
        cls, 
        /, 
        urls: (
            str | SupportsGeturl | URL | 
            Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL] | 
            Callable[[], Awaitable[str]] | Callable[[], Awaitable[SupportsGeturl]] | Callable[[], Awaitable[URL]] | 
            tuple[int, str] | tuple[int, SupportsGeturl] | tuple[int, URL] | 
            tuple[int, Callable[[], str]] | tuple[int, Callable[[], SupportsGeturl]] | tuple[int, Callable[[], URL]] | 
            tuple[int, Callable[[], Awaitable[str]]] | tuple[int, Callable[[], Awaitable[SupportsGeturl]]] | tuple[int, Callable[[], Awaitable[URL]]] | 
            Iterable[
                str | SupportsGeturl | URL | 
                Callable[[], str] | Callable[[], SupportsGeturl] | Callable[[], URL] | 
                Callable[[], Awaitable[str]] | Callable[[], Awaitable[SupportsGeturl]] | Callable[[], Awaitable[URL]] | 
                tuple[int, str] | tuple[int, SupportsGeturl] | tuple[int, URL] | 
                tuple[int, Callable[[], str]] | tuple[int, Callable[[], SupportsGeturl]] | tuple[int, Callable[[], URL]] | 
                tuple[int, Callable[[], Awaitable[str]]] | tuple[int, Callable[[], Awaitable[SupportsGeturl]]] | tuple[int, Callable[[], Awaitable[URL]]]
            ]
        ), 
        start: int = 0, 
        seek_threshold: int = 1 << 20, 
        request: None | Callable[..., Awaitable[Response]] = None, 
        get_file: None | str | Callable[[Response], Any] = None, 
        **request_kwargs, 
    ) -> Self:
        self = cls.__new__(cls)
        await self.__ainit__(
            urls, 
            start=start, 
            seek_threshold=seek_threshold, 
            request=request, 
            get_file=get_file, 
            **request_kwargs, 
        )
        return self

    def __repr__(self):
        cls = type(self)
        kwargs = {
            "urls": [(d["length"], d["url"]) for d in self._parts], 
            "start": self._pos, 
            "seek_threshold": self.seek_threshold, 
            "request": self.request, 
            "get_file": self.get_file, 
            **self.request_kwargs, 
        }
        return f"{cls.__module__}.{cls.__qualname__}({', '.join(map('%s=%r'.__mod__, kwargs.items()))})"

    @cached_property
    def response(self, /):
        return self.file.response

    async def aclose(self, /):
        await self.file.aclose()
        self._closed = True

    def close(self, /):
        self.file.close()
        self._closed = True

    async def read(self, size: None | int = -1, /) -> bytes: # type: ignore
        if size is None or size < 0:
            size = self.length - self._pos
        else:
            size = min(self.length - self._pos, size)
        if size <= 0:
            return b""
        buf = bytearray(size)
        await self.readinto(buf)
        return bytes(buf)

    async def readinto(self, buffer: Buffer, /) -> int: # type: ignore
        pos = self._pos
        remaining = min(self.length - pos, buffer_length(buffer))
        if remaining <= 0:
            return 0
        if "file" not in self.__dict__ or self.closed or self.response_closed:
            await self.reconnect(pos)
        try:
            readinto = self.file.readinto
            view = to_bytes_view(buffer)[:remaining]
            size = 0
            while size < remaining:
                start_pos = pos + size
                await self.seek(start_pos)
                size += await readinto(view[size:])
            return size
        except:
            self._pos = pos
            await self.aclose()
            raise

    async def readline(self, size: None | int = -1, /) -> bytes: # type: ignore
        pos = self._pos
        if size is None or size < 0:
            size = self.length - pos
        else:
            size = min(self.length - pos, size)
        if size <= 0:
            return b""
        if "file" not in self.__dict__ or self.closed or self.response_closed:
            await self.reconnect(pos)
        try:
            readline = self.file.readline
            buf = bytearray()
            while size > 0:
                if line := await readline(size):
                    buf += line
                    if buf.endswith(b"\n"):
                        break
                else:
                    await self.seek(pos + len(buf))
                size -= len(line)
            return bytes(buf)
        except:
            self._pos = pos
            await self.aclose()
            raise

    async def readlines(self, hint: int = -1, /) -> list[bytes]: # type: ignore
        pos = self._pos
        readline = self.readline
        ls: list[bytes]
        add = ls.append
        try:
            if hint <= 0:
                while l := await readline():
                    add(l)
            else:
                while hint >=0. and (l := await readline()):
                    add(l)
                    hint -= len(l)
            return ls
        except:
            self._pos = pos
            await self.aclose()
            raise

    async def read_range(self, size: None | int = -1, /, offset: int = 0) -> bytes:
        if offset < 0:
            offset += self.length
            if offset < 0:
                offset = 0
        if size is None or size < 0:
            size = self.length - offset
        else:
            size = min(self.length - offset, size)
        if size <= 0:
            return b""
        buf = bytearray(size)
        await self.readinto_range(buf, offset)
        return bytes(buf)

    async def readinto_range(self, buffer: Buffer, /, offset: int = 0):
        from urllib3_future_request import request_async

        if offset < 0:
            offset += self.length
            if offset < 0:
                offset = 0
        remaining = min(self.length - offset, buffer_length(buffer))
        if remaining <= 0:
            return 0
        request_kwargs = dict(self.request_kwargs)
        headers = request_kwargs["headers"] = dict(request_kwargs.get("headers") or ())
        headers["accept-encoding"] = "identity"
        view = to_bytes_view(buffer)[:remaining]
        size = 0
        while size < remaining:
            for vol in self._parts:
                if vol["start"] <= offset < vol["stop"]:
                    vol_offset = offset + size - vol["start"]
                    headers["range"] = f"bytes={vol_offset}-"
                    resp = await request_async(vol["url"], **request_kwargs)
                    try:
                        if vol_offset and resp.headers.get("accept-ranges") != "bytes":
                            raise OSError(29, "non-seekable stream")
                        size += await resp.readinto(view[size:])
                    finally:
                        await resp.close()
        return size

    async def reconnect(self, /, start: None | int = None) -> int:
        if start is None:
            start = self._pos
        elif start < 0:
            start += self.length
            if start < 0:
                start = 0
        if start >= self.length:
            return start
        for vol in self._parts:
            if vol["start"] <= start < vol["stop"]:
                await self.aclose()
                file = self.file = await AsyncHTTPFileReader.new(
                    vol["url"], 
                    start=vol["start"] - start, 
                    seek_threshold=self.seek_threshold, 
                    request=self.request, 
                    get_file=self.get_file, 
                    **self.request_kwargs, 
                )
                self.response = file.response
                self._pos = start
                self._closed = False
                return start
        return start

    async def seek(self, pos: int, whence: int = 0, /) -> int: # type: ignore
        old_pos = self._pos
        match whence:
            case 1:
                pos = old_pos + pos
            case 2:
                pos += self.length
        if pos < 0:
            pos = 0
        if pos == old_pos:
            return pos
        elif pos >= self.length:
            await self.aclose()
            self._pos = pos
            return pos
        if hasattr(self, "file"):
            file = self.file
            start = old_pos - file._pos
            stop = start + file.length
            if start <= pos < stop:
                await file.seek(pos-start)
                self._pos = pos
                return pos
        return await self.reconnect(pos)

# TODO: 设计实现一个 HTTPFileWriter，用于实现上传，关闭后视为上传完成
