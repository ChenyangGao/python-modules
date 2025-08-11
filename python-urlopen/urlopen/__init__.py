#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 1, 3)
__all__ = ["urlopen", "request", "download"]

import errno

from collections import UserString
from collections.abc import Buffer, Callable, Generator, Iterable, Mapping
from copy import copy
from http.client import HTTPResponse
from http.cookiejar import CookieJar
from inspect import isgenerator
from os import fsdecode, fstat, makedirs, PathLike
from os.path import abspath, dirname, isdir, join as joinpath
from shutil import COPY_BUFSIZE # type: ignore
from socket import getdefaulttimeout, setdefaulttimeout
from ssl import SSLContext, _create_unverified_context
from types import EllipsisType
from typing import cast, overload, Any, Literal
from urllib.error import HTTPError
from urllib.request import (
    build_opener, BaseHandler, HTTPCookieProcessor, HTTPSHandler, 
    HTTPRedirectHandler, OpenerDirector, Request, 
)

from argtools import argcount
from cookietools import cookies_dict_to_str
from dicttools import iter_items
from filewrap import bio_skip_iter, bio_chunk_iter, SupportsRead, SupportsWrite
from http_request import normalize_request_args, SupportsGeturl
from http_response import (
    decompress_response, get_filename, get_length, is_chunked, is_range_request, 
    parse_response, 
)
from yarl import URL
from undefined import undefined, Undefined


type string = Buffer | str | UserString

if "__del__" not in HTTPResponse.__dict__:
    setattr(HTTPResponse, "__del__", HTTPResponse.close)
if "__del__" not in OpenerDirector.__dict__:
    setattr(OpenerDirector, "__del__", OpenerDirector.close)

_cookies = CookieJar()
_opener: OpenerDirector = build_opener(HTTPSHandler(context=_create_unverified_context()), HTTPCookieProcessor(_cookies))
setattr(_opener, "cookies", _cookies)


if getdefaulttimeout() is None:
    setdefaulttimeout(60)


class NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, /, *args, **kwds):
        return None


def urlopen(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    proxies: None | Mapping[str, str] | Iterable[tuple[str, str]] = None, 
    context: None | SSLContext = None, 
    cookies: None | CookieJar = None, 
    timeout: None | Undefined | float = undefined, 
    opener: None | OpenerDirector = _opener, 
    **_, 
) -> HTTPResponse:
    if isinstance(url, Request):
        request = url
    else:
        if isinstance(data, PathLike):
            data = bio_chunk_iter(open(data, "rb"))
        elif isinstance(data, SupportsRead):
            data = bio_chunk_iter(data)
        request = Request(**normalize_request_args( # type: ignore
            method=method, 
            url=url, 
            params=params, 
            data=data, 
            json=json, 
            files=files, 
            headers=headers, 
            ensure_ascii=True, 
        ))
        if proxies:
            for host, type in iter_items(proxies):
                request.set_proxy(host, type)
    headers_ = request.headers
    if opener is None:
        handlers: list[BaseHandler] = []
    else:
        handlers = list(map(copy, getattr(opener, "handlers")))
        if cookies is None:
            cookies = getattr(opener, "cookies", None)
    if cookies and "cookie" not in headers_:
        headers_["cookie"] = cookies_dict_to_str(cookies)
    if context is not None:
        handlers.append(HTTPSHandler(context=context))
    elif opener is None:
        handlers.append(HTTPSHandler(context=_create_unverified_context()))
    if cookies is not None and (opener is None or all(
        h.cookiejar is not cookies 
        for h in getattr(opener, "handlers") if isinstance(h, HTTPCookieProcessor)
    )):
        handlers.append(HTTPCookieProcessor(cookies))
    response_cookies = CookieJar()
    if cookies is None:
        cookies = response_cookies
    handlers.append(HTTPCookieProcessor(response_cookies))
    if not follow_redirects:
        handlers.append(NoRedirectHandler())
    opener = build_opener(*handlers)
    setattr(opener, "cookies", cookies)
    try:
        if timeout is undefined:
            response = opener.open(request)
        else:
            response = opener.open(request, timeout=cast(None|float, timeout))
        setattr(response, "opener", opener)
        setattr(response, "cookies", response_cookies)
        return response
    except HTTPError as e:
        if response := getattr(e, "file", None):
            setattr(response, "opener", opener)
            setattr(response, "cookies", response_cookies)
        raise


@overload
def request(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
) -> HTTPResponse:
    ...
@overload
def request(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> bytes:
    ...
@overload
def request(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    *, 
    parse: Literal[True], 
    **request_kwargs, 
) -> bytes | str | dict | list | int | float | bool | None:
    ...
@overload
def request[T](
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    *, 
    parse: Callable[[HTTPResponse, bytes], T] | Callable[[HTTPResponse], T], 
    **request_kwargs, 
) -> T:
    ...
def request[T](
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    *, 
    parse: None | EllipsisType| bool | Callable[[HTTPResponse, bytes], T] | Callable[[HTTPResponse], T] = None, 
    **request_kwargs, 
) -> HTTPResponse | bytes | str | dict | list | int | float | bool | None | T:
    try:
        response = urlopen(
            url=url, 
            method=method, 
            params=params, 
            data=data, 
            json=json, 
            files=files, 
            headers=headers, 
            follow_redirects=follow_redirects, 
            **request_kwargs, 
        )
    except HTTPError as e:
        if raise_for_status:
            raise
        response = getattr(e, "file")
    if parse is None:
        return response
    elif parse is ...:
        response.close()
        return response
    with response:
        if isinstance(parse, bool):
            data = decompress_response(response.read(), response)
            if parse:
                return parse_response(response, data)
            return data
        ac = argcount(parse)
        if ac == 1:
            return cast(Callable[[HTTPResponse], T], parse)(response)
        else:
            data = decompress_response(response.read(), response)
            return cast(Callable[[HTTPResponse, bytes], T], parse)(response, data)


def download(
    url: string | SupportsGeturl | URL | Request, 
    file: bytes | str | PathLike | SupportsWrite[bytes] = "", 
    resume: bool = False, 
    chunksize: int = COPY_BUFSIZE, 
    headers: None | Mapping[str, str] | Iterable[tuple[str, str]] = None, 
    make_reporthook: None | Callable[[None | int], Callable[[int], Any] | Generator[int, Any, Any]] = None, 
    **request_kwargs, 
) -> str | SupportsWrite[bytes]:
    """Download a URL into a file.

    Example::

        1. use `make_reporthook` to show progress:

            You can use the following function to show progress for the download task

            .. code: python

                from time import perf_counter

                def progress(total=None):
                    read_num = 0
                    start_t = perf_counter()
                    while True:
                        read_num += yield
                        speed = read_num / 1024 / 1024 / (perf_counter() - start_t)
                        print(f"\r\x1b[K{read_num} / {total} | {speed:.2f} MB/s", end="", flush=True)

            Or use the following function for more real-time speed

            .. code: python

                from collections import deque
                from time import perf_counter
    
                def progress(total=None):
                    dq = deque(maxlen=64)
                    read_num = 0
                    dq.append((read_num, perf_counter()))
                    while True:
                        read_num += yield
                        cur_t = perf_counter()
                        speed = (read_num - dq[0][0]) / 1024 / 1024 / (cur_t - dq[0][1])
                        print(f"\r\x1b[K{read_num} / {total} | {speed:.2f} MB/s", end="", flush=True)
                        dq.append((read_num, cur_t))
    """
    if chunksize <= 0:
        chunksize = COPY_BUFSIZE
    headers = request_kwargs["headers"] = dict(headers or ())
    headers["accept-encoding"] = "identity"
    response: HTTPResponse = urlopen(url, **request_kwargs)
    content_length = get_length(response)
    if content_length == 0 and is_chunked(response):
        content_length = None
    fdst: SupportsWrite[bytes]
    if hasattr(file, "write"):
        file = fdst = cast(SupportsWrite[bytes], file)
    else:
        file = abspath(fsdecode(file))
        if isdir(file):
            file = joinpath(file, get_filename(response, "download"))
        try:
            fdst = open(file, "ab" if resume else "wb")
        except FileNotFoundError:
            makedirs(dirname(file), exist_ok=True)
            fdst = open(file, "ab" if resume else "wb")
    filesize = 0
    if resume:
        try:
            fileno = getattr(fdst, "fileno")()
            filesize = fstat(fileno).st_size
        except (AttributeError, OSError):
            pass
        else:
            if filesize == content_length:
                return file
            if filesize and is_range_request(response):
                if filesize == content_length:
                    return file
            elif content_length is not None and filesize > content_length:
                raise OSError(
                    errno.EIO, 
                    f"file {file!r} is larger than url {url!r}: {filesize} > {content_length} (in bytes)", 
                )
    reporthook_close: None | Callable = None
    if callable(make_reporthook):
        reporthook = make_reporthook(content_length)
        if isgenerator(reporthook):
            reporthook_close = reporthook.close
            next(reporthook)
            reporthook = reporthook.send
        else:
            reporthook_close = getattr(reporthook, "close", None)
        reporthook = cast(Callable[[int], Any], reporthook)
    else:
        reporthook = None
    try:
        if filesize:
            if is_range_request(response):
                response.close()
                response = urlopen(url, headers={**headers, "Range": "bytes=%d-" % filesize}, **request_kwargs)
                if not is_range_request(response):
                    raise OSError(errno.EIO, f"range request failed: {url!r}")
                if reporthook is not None:
                    reporthook(filesize)
            elif resume:
                for _ in bio_skip_iter(response, filesize, callback=reporthook):
                    pass

        fsrc_read = response.read 
        fdst_write = fdst.write
        while (chunk := fsrc_read(chunksize)):
            fdst_write(chunk)
            if reporthook is not None:
                reporthook(len(chunk))
    finally:
        response.close()
        if callable(reporthook_close):
            reporthook_close()
    return file

