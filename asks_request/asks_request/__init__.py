#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 2)
__all__ = ["request"]

from collections import UserString
from collections.abc import Buffer, Callable, Iterable, Mapping
from http.cookiejar import CookieJar
from http.cookies import BaseCookie
from inspect import isawaitable
from os import PathLike
from types import EllipsisType
from typing import cast, overload, Any, Final, Literal

from argtools import argcount
from cookietools import cookies_to_dict, update_cookies
from dicttools import get_all_items
from ensure import ensure_acm
from filewrap import bio_chunk_async_iter, SupportsRead
from http_request import normalize_request_args, SupportsGeturl
from http_response import parse_response
from asks.response_objects import BaseResponse, StreamBody # type: ignore
from asks.sessions import Session # type: ignore
from yarl import URL


type string = Buffer | str | UserString

_REQUEST_KWARGS: Final = {
    "method", "url", "data", "params", "headers", "encoding", "json", "files", 
    "multipart", "cookies", "callback", "timeout", "retries", "max_redirects", 
    "follow_redirects", "persist_cookies", "auth", "stream", 
}

def __del__(self, /):
    from asynctools import run_async
    run_async(self.close())

if "__del__" not in Session.__dict__:
    setattr(Session, "__del__", __del__)
if "__del__" not in StreamBody.__dict__:
    setattr(StreamBody, "__del__", __del__)

def bugfix():
    import h11

    from asks.http_utils import decompress, parse_content_encoding # type: ignore
    from asks.response_objects import decompress, StreamBody

    async def __aiter__(self):
        if self.content_encoding is not None:
            decompressor = decompress(parse_content_encoding(self.content_encoding))
        while True:
            event = await self._recv_event()
            if isinstance(event, h11.Data):
                data = event.data
                if self.content_encoding is not None:
                    if self.decompress_data:
                        data = decompressor.send(data)
                yield data
            elif isinstance(event, h11.EndOfMessage):
                break

    StreamBody.__aiter__ = __aiter__

bugfix()

_DEFAULT_SESSION = Session(connections=128)


@overload
async def request(
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Session = _DEFAULT_SESSION, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
) -> BaseResponse:
    ...
@overload
async def request(
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Session = _DEFAULT_SESSION, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> bytes:
    ...
@overload
async def request(
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Session = _DEFAULT_SESSION, 
    *, 
    parse: Literal[True], 
    **request_kwargs, 
) -> bytes | str | dict | list | int | float | bool | None:
    ...
@overload
async def request[T](
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Session = _DEFAULT_SESSION, 
    *, 
    parse: Callable[[BaseResponse, bytes], T] | Callable[[BaseResponse], T], 
    **request_kwargs, 
) -> T:
    ...
async def request[T](
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Session = _DEFAULT_SESSION, 
    *, 
    parse: None | EllipsisType| bool | Callable[[BaseResponse, bytes], T] | Callable[[BaseResponse], T] = None, 
    **request_kwargs, 
) -> BaseResponse | bytes | str | dict | list | int | float | bool | None | T:
    request_kwargs["follow_redirects"] = follow_redirects
    request_kwargs["stream"] = stream
    if session is None:
        session = Session()
    if isinstance(data, PathLike):
        data = bio_chunk_async_iter(open(data, "rb"))
    elif isinstance(data, SupportsRead):
        data = bio_chunk_async_iter(data)
    request_kwargs.update(normalize_request_args(
        method=method, 
        url=url, 
        params=params, 
        data=data, 
        json=json, 
        files=files, 
        headers=headers, 
        async_=True, 
    ))
    if cookies is not None:
        request_kwargs["cookies"] = cookies_to_dict(cookies, predicate=request_kwargs["url"])
    response = await session.request(
        **dict(get_all_items(request_kwargs, *_REQUEST_KWARGS)))
    setattr(response, "session", session)
    if cookies is not None and response.cookies:
        update_cookies(cookies, response.cookies) # type: ignore
    if raise_for_status:
        response.raise_for_status()
    if parse is None:
        return response
    elif parse is ...:
        body = response.body
        if not isinstance(body, Buffer):
            await body.close()
        return response
    async with ensure_acm(response.body):
        if isinstance(parse, bool):
            content = response.body
            if not isinstance(content, Buffer):
                async with content as chunks:
                    content = bytearray()
                    async for chunk in chunks:
                        content += chunk
            if parse:
                return parse_response(response, content)
            return content
        ac = argcount(parse)
        if ac == 1:
            ret = cast(Callable[[BaseResponse], T], parse)(response)
        else:
            content = response.body
            if not isinstance(content, Buffer):
                async with content as chunks:
                    content = bytearray()
                    async for chunk in chunks:
                        content += chunk
            ret = cast(Callable[[BaseResponse, bytes], T], parse)(
                response, content)
        if isawaitable(ret):
            ret = await ret
        return ret

