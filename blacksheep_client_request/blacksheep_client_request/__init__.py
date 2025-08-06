#!/usr/bin/env python3
# coding: utf-8

from __future__ import annotations

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 4)
__all__ = ["request"]

from collections import UserString
from collections.abc import AsyncGenerator, AsyncIterable, Buffer, Callable, Iterable, Mapping
from gzip import decompress as decompress_gzip
from http.cookiejar import CookieJar
from http.cookies import SimpleCookie
from inspect import isawaitable
from os import PathLike
from types import EllipsisType
from typing import cast, Any, Literal
from zlib import compressobj, DEF_MEM_LEVEL, DEFLATED, MAX_WBITS

from argtools import argcount
from blacksheep.client.session import ClientSession
from blacksheep.client.cookies import Cookie as BlackSheepCookie, CookieJar as BlackSheepCookieJar
from blacksheep.client.exceptions import UnsupportedRedirect
from blacksheep.common.types import normalize_headers
from blacksheep.contents import Content, StreamedContent
from blacksheep.exceptions import HTTPException
from blacksheep.messages import Request, Response
from cookietools import update_cookies
from ensure import ensure_buffer
from filewrap import SupportsRead
from http_request import normalize_request_args, SupportsGeturl
from http_response import parse_response
from filewrap import bio_chunk_async_iter
from multidict import CIMultiDict
from yarl import URL
from undefined import undefined, Undefined


type string = Buffer | str | UserString
_DEFAULT_SESSION: ClientSession
COOKIE_ATTRS =signature(Cookie).parameters.keys()


def _get_default_session() -> ClientSession:
    global _DEFAULT_SESSION
    try:
        return _DEFAULT_SESSION
    except NameError:
        _DEFAULT_SESSION = ClientSession(follow_redirects=False)
        return _DEFAULT_SESSION


if "__del__" not in ClientSession.__dict__:
    def close(self, /):
        from asynctools import run_async
        try:
            run_async(ClientSession.close(self))
        except:
            pass
    setattr(ClientSession, "__del__", close)


def decompress_deflate(data: bytes, compresslevel: int = 9) -> bytes:
    # Fork from: https://stackoverflow.com/questions/1089662/python-inflate-and-deflate-implementations#answer-1089787
    compress = compressobj(
            compresslevel,  # level: 0-9
            DEFLATED,       # method: must be DEFLATED
            -MAX_WBITS,     # window size in bits:
                            #   -15..-8: negate, suppress header
                            #   8..15: normal
                            #   16..30: subtract 16, gzip header
            DEF_MEM_LEVEL,  # mem level: 1..8/9
            0               # strategy:
                            #   0 = Z_DEFAULT_STRATEGY
                            #   1 = Z_FILTERED
                            #   2 = Z_HUFFMAN_ONLY
                            #   3 = Z_RLE
                            #   4 = Z_FIXED
    )
    deflated = compress.compress(data)
    deflated += compress.flush()
    return deflated


async def decompress_response(response: ResponseWrapper, /) -> bytes:
    data = await response.read()
    content_encoding = response.headers.get("Content-Encoding")
    match content_encoding:
        case "gzip":
            data = decompress_gzip(data)
        case "deflate":
            data = decompress_deflate(data)
        case "br":
            from brotli import decompress as decompress_br # type: ignore
            data = decompress_br(data)
        case "zstd":
            from zstandard import decompress as decompress_zstd
            data = decompress_zstd(data)
    return data


class ResponseWrapper:

    def __init__(self, response: Response):
        self.response = response
        self.headers = CIMultiDict((str(k, "latin-1"), str(v, "latin-1")) for k, v in response.headers)

    def __dir__(self, /) -> list[str]:
        s = set(super().__dir__())
        s.update(dir(self.response))
        return sorted(s)

    def __getattr__(self, attr, /):
        return getattr(self.response, attr)

    def __repr__(self, /):
        return f"{type(self).__qualname__}({self.response!r})"


async def request(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | SimpleCookie | BlackSheepCookieJar = None, 
    session: None | Undefined | ClientSession = undefined, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
):
    request_kwargs.pop("stream", None)
    if session is undefined:
        session = _get_default_session()
    elif session is None:
        session = ClientSession()
    session = cast(ClientSession, session)
    session.follow_redirects = False
    if isinstance(url, Request):
        request = url
    else:
        if isinstance(data, Content):
            request_args = normalize_request_args(
                method=method, 
                url=url, 
                params=params, 
                headers=headers, 
            )
            request_args["data"] = data
        else:
            if isinstance(data, PathLike):
                data = bio_chunk_async_iter(open(data, "rb"))
            elif isinstance(data, SupportsRead):
                data = bio_chunk_async_iter(data)
            request_args = normalize_request_args(
                method=method, 
                url=url, 
                params=params, 
                data=data, 
                json=json, 
                headers=headers, 
            )
        headers_ = request_args["headers"] or {}
        request = Request(
            request_args["method"], 
            bytes(request_args["url"], "utf-8"), 
            normalize_headers(headers_), 
        )
        if data := request_args["data"]:
            content_type = bytes(headers_.get("content-type") or "application/octet-stream", "latin-1")
            if isinstance(data, Content):
                pass
            elif isinstance(data, Buffer):
                data = Content(content_type, data)
            else:
                if not isinstance(data, AsyncGenerator):
                    async def as_gen(data, /):
                        if isinstance(data, AsyncIterable):
                            async for chunk in data:
                                yield ensure_buffer(chunk)
                        else:
                            for chunk in data:
                                yield ensure_buffer(chunk)
                    data = as_gen(data)
                data = StreamedContent(content_type, data)
        request = request.with_content(data)
    if cookies is not None:
        if isinstance(cookies, SimpleCookie):
            request.cookies.update((name, BlackSheepCookie(name, morsel.value)) for name, morsel in cookies.items())
        else:
            request.cookies.update((cookie.name, BlackSheepCookie(cookie.name, cookie.value)) for cookie in cookies)
    while True:
        response = ResponseWrapper(await session.send(request))
        resp_cookies = response.cookies
        if cookies is not None and resp_cookies:
            update_cookies(cookies, resp_cookies) # type: ignore
        if response.status >= 400 and raise_for_status:
            raise HTTPException(response.status, response.reason)
        if follow_redirects and response.is_redirect():
            try:
                session.update_request_for_redirect(request, response)
                continue
            except UnsupportedRedirect:
                pass
        if parse is None or parse is ...:
            return response
        elif parse is False:
            return await decompress_response(response)
        if isinstance(response, bool):
            data = await decompress_response(response)
            if parse:
                return parse_response(parse_response, data)
            return data
        ac = argcount(parse)
        if ac == 1:
            ret = parse(response)
        else:
            ret = parse(response, await decompress_response(response))
        if isawaitable(ret):
            ret = await ret
        return ret

