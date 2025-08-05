#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 9)
__all__ = [
    "SupportsGeturl", "url_origin", "complete_url", "ensure_ascii_url", 
    "urlencode", "cookies_str_to_dict", "headers_str_to_dict_by_lines", 
    "headers_str_to_dict", "encode_multipart_data", "encode_multipart_data_async", 
    "normalize_request_args", 
]

from collections import UserString
from collections.abc import (
    AsyncIterable, AsyncIterator, Buffer, Iterable, Iterator, 
    Mapping, Sequence, 
)
from decimal import Decimal
from fractions import Fraction
from io import TextIOWrapper
from itertools import batched
from mimetypes import guess_type
from numbers import Integral, Real
from os import PathLike
from os.path import basename
from re import compile as re_compile, Pattern
from string import punctuation
from typing import runtime_checkable, Any, Final, Protocol, TypedDict
from urllib.parse import quote, urlparse, urlunparse
from uuid import uuid4
from yarl import URL

from asynctools import async_map
from dicttools import dict_map, iter_items
from ensure import ensure_bytes, ensure_buffer, ensure_str
from filewrap import bio_chunk_iter, bio_chunk_async_iter, SupportsRead
from http_response import get_charset, get_mimetype
from orjson import dumps as json_dumps
from texttools import text_to_dict


type string = Buffer | str | UserString

QUERY_KEY_TRANSTAB: Final = {k: f"%{k:02X}" for k in b"&="}
CRE_URL_SCHEME_match: Final = re_compile(r"(?i:[a-z][a-z0-9.+-]*)://").match


class RequestArgs(TypedDict):
    method: str
    url: str
    data: Buffer | Iterable[Buffer] | AsyncIterable[Buffer]
    headers: dict[str, str]


@runtime_checkable
class SupportsGeturl[AnyStr: (bytes, str)](Protocol):
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
    urlp = urlparse(url)
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
    urlp = urlparse(url)
    repl = {"query": "", "fragment": ""}
    if not urlp.scheme:
        repl["scheme"] = "http"
    netloc = urlp.netloc
    if not netloc:
        netloc = "localhost"
    if default_port and not urlp.port:
        netloc = netloc.removesuffix(":") + f":{default_port}"
    repl["netloc"] = netloc
    return urlunparse(urlp._replace(**repl)).rstrip("/")


def ensure_ascii_url(url: str, /) -> str:
    if url.isascii():
        return url
    return quote(url, safe=punctuation)


def urlencode(
    payload: string | Mapping[Any, Any] | Iterable[tuple[Any, Any]], 
    /, 
    encoding: str = "utf-8", 
    errors: str = "strict", 
) -> str:
    if isinstance(payload, str):
        return payload
    elif isinstance(payload, UserString):
        return str(payload)
    elif isinstance(payload, Buffer):
        return str(payload, encoding, errors)
    def encode_iter(payload: Iterable[tuple[Any, Any]], /) -> Iterator[str]:
        for i, (k, v) in enumerate(payload):
            if i:
                yield "&"
            if isinstance(k, Buffer):
                k = str(k, encoding, errors)
            else:
                k = str(k)
            yield k.translate(QUERY_KEY_TRANSTAB)
            yield "="
            if v is True:
                yield "true"
            elif v is False:
                yield "false"
            elif v is None:
                yield "null"
            elif isinstance(v, (str, UserString)):
                pass
            elif isinstance(v, Buffer):
                v = str(v, encoding, errors)
            elif isinstance(v, (Mapping, Iterable)):
                v = json_dumps(v, default=json_default).decode("utf-8")
            else:
                v = str(v)
            yield v.replace("&", "%26")
    return "".join(encode_iter(iter_items(payload)))


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


def json_default(o, /):
    if isinstance(o, Mapping):
        return dict(o)
    elif isinstance(o, Buffer):
        return ensure_str(o)
    elif isinstance(o, UserString):
        return str(o)
    elif isinstance(o, Integral):
        return int(o)
    elif isinstance(o, (Real, Fraction, Decimal)):
        try:
            return float(o)
        except Exception:
            return str(o)
    elif isinstance(o, (Iterator, Sequence)):
        return list(o)
    else:
        return str(o)


def normalize_request_args(
    method: string, 
    url: string | SupportsGeturl | URL, 
    params: Any = None, 
    data: Any = None, 
    json: Any = None, 
    headers: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    ensure_ascii: bool = False, 
) -> RequestArgs:
    method = ensure_str(method).upper()
    if isinstance(url, SupportsGeturl):
        url = url.geturl()
    elif isinstance(url, URL):
        url = str(url)
    url = complete_url(ensure_str(url))
    if params and (params := urlencode(params)):
        urlp = urlparse(url)
        if query := urlp.query:
            params = query + "&" + params
        url = urlunparse(urlp._replace(query=params))
    if ensure_ascii:
        url = ensure_ascii_url(url)
    headers_ = dict_map(
        headers or (), 
        key=lambda k: ensure_str(k).lower(), 
        value=ensure_str, 
    )
    content_type = headers_.get("content-type", "")
    charset      = get_charset(content_type)
    mimetype     = get_mimetype(charset).lower()
    if data is not None:
        if isinstance(data, Buffer):
            pass
        elif isinstance(data, (str, UserString)):
            data = data.encode(charset)
        elif isinstance(data, AsyncIterable):
            data = async_map(ensure_buffer, data)
        elif isinstance(data, Iterator):
            data = map(ensure_buffer, data)
        elif mimetype == "application/json":
            if charset == "utf-8":
                data = json_dumps(data, default=json_default)
            else:
                from json import dumps
                data = dumps(data, default=json_default).encode(charset)
        elif isinstance(data, (Mapping, Sequence)):
            if data:
                data = urlencode(data, charset).encode(charset)
                if mimetype != "application/x-www-form-urlencoded":
                    headers_["content-type"] = "application/x-www-form-urlencoded"
        else:
            data = str(data).encode(charset)
    elif json is not None:
        if isinstance(json, Buffer):
            data = json
        elif isinstance(data, AsyncIterable):
            data = async_map(ensure_buffer, data)
        if charset == "utf-8":
            data = json_dumps(data, default=json_default)
        else:
            from json import dumps
            data = dumps(data, default=json_default).encode(charset)
        if mimetype != "application/json":
            headers_["content-type"] = "application/json; charset=" + charset
    elif mimetype == "application/json":
        data = b"null"
    return {
        "url": url, 
        "method": method, 
        "data": data, 
        "headers": headers_
    }

