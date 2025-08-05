#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 8)
__all__ = [
    "SupportsGeturl", "url_origin", "complete_url", "cookies_str_to_dict", 
    "headers_str_to_dict_by_lines", "headers_str_to_dict", 
    "encode_multipart_data", "encode_multipart_data_async", 
]

from collections import UserString
from collections.abc import (
    AsyncIterable, AsyncIterator, Buffer, Iterable, Iterator, Mapping, 
)
from io import TextIOWrapper
from itertools import batched
from mimetypes import guess_type
from os import PathLike
from os.path import basename
from re import compile as re_compile, Pattern
from typing import runtime_checkable, Any, Final, Protocol, TypeVar
from urllib.parse import quote, urlsplit, urlunsplit
from uuid import uuid4

from asynctools import async_map
from dicttools import iter_items
from filewrap import bio_chunk_iter, bio_chunk_async_iter, SupportsRead
from integer_tool import int_to_bytes
from texttools import text_to_dict


AnyStr = TypeVar("AnyStr", bytes, str, covariant=True)

CRE_URL_SCHEME_match: Final = re_compile(r"(?i:[a-z][a-z0-9.+-]*)://").match


@runtime_checkable
class SupportsGeturl(Protocol[AnyStr]):
    def geturl(self) -> AnyStr: ...


def url_origin(url: str, /, default_port: int = 0) -> str:
    if url.startswith("/"):
        url = "http://localhost" + url
    elif url.startswith("//"):
        url = "http:" + url
    elif url.startswith("://"):
        url = "http" + url
    elif not CRE_URL_SCHEME_match(url):
        url = "http://" + url
    urlp = urlsplit(url)
    scheme, netloc = urlp.scheme or "http", urlp.netloc or "localhost"
    if default_port and not urlp.port:
        netloc = netloc.removesuffix(":") + f":{default_port}"
    return f"{scheme}://{netloc}"


def complete_url(url: str, /, default_port: int = 0) -> str:
    if url.startswith("/"):
        url = "http://localhost" + url
    elif url.startswith("//"):
        url = "http:" + url
    elif url.startswith("://"):
        url = "http" + url
    elif not CRE_URL_SCHEME_match(url):
        url = "http://" + url
    urlp = urlsplit(url)
    repl = {"query": "", "fragment": ""}
    if not urlp.scheme:
        repl["scheme"] = "http"
    netloc = urlp.netloc
    if not netloc:
        netloc = "localhost"
    if default_port and not urlp.port:
        netloc = netloc.removesuffix(":") + f":{default_port}"
    repl["netloc"] = netloc
    return urlunsplit(urlp._replace(**repl)).rstrip("/")


def cookies_str_to_dict(
    cookies: str, 
    /, 
    kv_sep: str | Pattern[str] = re_compile(r"\s*=\s*"), 
    entry_sep: str | Pattern[str] = re_compile(r"\s*;\s*"), 
) -> dict[str, str]:
    return text_to_dict(cookies.strip(), kv_sep, entry_sep)


def headers_str_to_dict(
    headers: str, 
    /, 
    kv_sep: str | Pattern[str] = re_compile(r":\s+"), 
    entry_sep: str | Pattern[str] = re_compile("\n+"), 
) -> dict[str, str]:
    return text_to_dict(headers.strip(), kv_sep, entry_sep)


def headers_str_to_dict_by_lines(headers: str, /, ) -> dict[str, str]:
    lines = headers.strip().split("\n")
    if len(lines) & 1:
        lines.append("")
    return dict(batched(lines, 2)) # type: ignore


def ensure_bytes(
    o, 
    /, 
    encoding: str = "utf-8", 
    errors: str = "strict", 
) -> bytes:
    if isinstance(o, bytes):
        return o
    elif isinstance(o, memoryview):
        return o.tobytes()
    elif isinstance(o, Buffer):
        return bytes(o)
    elif isinstance(o, int):
        return int_to_bytes(o)
    elif isinstance(o, (str, UserString)):
        return o.encode(encoding, errors)
    try:
        return bytes(o)
    except TypeError:
        return bytes(str(o), encoding, errors)


def ensure_buffer(
    o, 
    /, 
    encoding: str = "utf-8", 
    errors: str = "strict", 
) -> Buffer:
    if isinstance(o, Buffer):
        return o
    elif isinstance(o, int):
        return int_to_bytes(o)
    elif isinstance(o, (str, UserString)):
        return o.encode(encoding, errors)
    try:
        return bytes(o)
    except TypeError:
        return bytes(str(o), encoding, errors)


def encode_multipart_data(
    data: None | Mapping[Buffer | str, Any] = None, 
    files: None | Mapping[Buffer | str, Any] = None, 
    boundary: None | str = None, 
    file_suffix: str = "", 
) -> tuple[dict, Iterator[Buffer]]:
    if not boundary:
        boundary = uuid4().hex
        boundary_bytes = bytes(boundary, "ascii")
    elif isinstance(boundary, str):
        boundary_bytes = bytes(boundary, "latin-1")
    else:
        boundary_bytes = bytes(boundary)
        boundary = str(boundary_bytes, "latin-1")
    boundary_line = b"--%s\r\n" % boundary_bytes
    suffix = ensure_bytes(file_suffix)
    if suffix and not suffix.startswith(b"."):
        suffix = b"." + suffix

    def encode_item(name, value, /, is_file=False) -> Iterator[Buffer]:
        headers = {b"content-disposition": b'form-data; name="%s"' % bytes(quote(name), "ascii")}
        filename = b""
        if isinstance(value, (list, tuple)):
            match value:
                case [value]:
                    pass
                case [_, value]:
                    pass
                case [_, value, file_type]:
                    if file_type:
                        headers[b"content-type"] = ensure_bytes(file_type)
                case [_, value, file_type, file_headers, *rest]:
                    for k, v in iter_items(file_headers):
                        headers[ensure_bytes(k).lower()] = ensure_bytes(v)
                    if file_type:
                        headers[b"content-type"] = ensure_bytes(file_type)
        if isinstance(value, (PathLike, SupportsRead)):
            is_file = True
            if isinstance(value, PathLike):
                file: SupportsRead[Buffer] = open(value, "rb")
            elif isinstance(value, TextIOWrapper):
                file = value.buffer
            else:
                file = value
            value = bio_chunk_iter(file)
            if not filename:
                filename = ensure_bytes(basename(getattr(file, "name", b"") or b""))
        elif isinstance(value, Buffer):
            pass
        elif isinstance(value, (str, UserString)):
            value = ensure_bytes(value)
        elif isinstance(value, Iterable):
            value = map(ensure_buffer, value)
        else:
            value = ensure_buffer(value)
        if is_file:
            if filename:
                filename = bytes(quote(filename), "ascii")
                if suffix and not filename.endswith(suffix):
                    filename += suffix
            else:
                filename = bytes(uuid4().hex, "ascii") + suffix
            if b"content-type" not in headers:
                headers[b"content-type"] = ensure_bytes(
                    guess_type(str(filename, "latin-1"))[0] or b"application/octet-stream")
            headers[b"content-disposition"] += b'; filename="%s"' % filename
        yield boundary_line
        for entry in headers.items():
            yield b"%s: %s\r\n" % entry
        yield b"\r\n"
        if isinstance(value, Buffer):
            yield value
        else:
            yield from value

    def encode_iter() -> Iterator[Buffer]:
        if data:
            for name, value in iter_items(data):
                yield boundary_line
                yield from encode_item(name, value)
                yield b"\r\n"
        if files:
            for name, value in iter_items(files):
                yield boundary_line
                yield from encode_item(name, value, is_file=True)
                yield b"\r\n"
        yield b'--%s--\r\n' % boundary_bytes

    return {"content-type": "multipart/form-data; boundary="+boundary}, encode_iter()


def encode_multipart_data_async(
    data: None | Mapping[Buffer | str, Any] = None, 
    files: None | Mapping[Buffer | str, Any] = None, 
    boundary: None | str = None, 
    file_suffix: str = "", 
) -> tuple[dict, AsyncIterator[Buffer]]:
    if not boundary:
        boundary = uuid4().hex
        boundary_bytes = bytes(boundary, "ascii")
    elif isinstance(boundary, str):
        boundary_bytes = bytes(boundary, "latin-1")
    else:
        boundary_bytes = bytes(boundary)
        boundary = str(boundary_bytes, "latin-1")
    boundary_line = b"--%s\r\n" % boundary_bytes
    suffix = ensure_bytes(file_suffix)
    if suffix and not suffix.startswith(b"."):
        suffix = b"." + suffix

    async def encode_item(name, value, /, is_file=False) -> AsyncIterator[Buffer]:
        headers = {b"content-disposition": b'form-data; name="%s"' % bytes(quote(name), "ascii")}
        filename = b""
        if isinstance(value, (list, tuple)):
            match value:
                case [value]:
                    pass
                case [_, value]:
                    pass
                case [_, value, file_type]:
                    if file_type:
                        headers[b"content-type"] = ensure_bytes(file_type)
                case [_, value, file_type, file_headers, *rest]:
                    for k, v in iter_items(file_headers):
                        headers[ensure_bytes(k).lower()] = ensure_bytes(v)
                    if file_type:
                        headers[b"content-type"] = ensure_bytes(file_type)
        if isinstance(value, (PathLike, SupportsRead)):
            is_file = True
            if isinstance(value, PathLike):
                file: SupportsRead[Buffer] = open(value, "rb")
            elif isinstance(value, TextIOWrapper):
                file = value.buffer
            else:
                file = value
            value = bio_chunk_async_iter(file)
            if not filename:
                filename = ensure_bytes(basename(getattr(file, "name", b"") or b""))
        elif isinstance(value, Buffer):
            pass
        elif isinstance(value, (str, UserString)):
            value = ensure_bytes(value)
        elif isinstance(value, Iterable):
            value = async_map(ensure_buffer, value)
        else:
            value = ensure_buffer(value)
        if is_file:
            if filename:
                filename = bytes(quote(filename), "ascii")
                if suffix and not filename.endswith(suffix):
                    filename += suffix
            else:
                filename = bytes(uuid4().hex, "ascii") + suffix
            if b"content-type" not in headers:
                headers[b"content-type"] = ensure_bytes(
                    guess_type(str(filename, "latin-1"))[0] or b"application/octet-stream")
            headers[b"content-disposition"] += b'; filename="%s"' % filename
        yield boundary_line
        for entry in headers.items():
            yield b"%s: %s\r\n" % entry
        yield b"\r\n"
        if isinstance(value, Buffer):
            yield value
        elif isinstance(value, AsyncIterable):
            async for line in value:
                yield line
        else:
            for line in value:
                yield line

    async def encode_iter() -> AsyncIterator[Buffer]:
        if data:
            for name, value in iter_items(data):
                yield boundary_line
                async for line in encode_item(name, value):
                    yield line
                yield b"\r\n"
        if files:
            for name, value in iter_items(files):
                yield boundary_line
                async for line in encode_item(name, value, is_file=True):
                    yield line
                yield b"\r\n"
        yield b'--%s--\r\n' % boundary_bytes

    return {"content-type": "multipart/form-data; boundary="+boundary}, encode_iter()

