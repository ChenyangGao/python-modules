#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 1, 3)
__all__ = ["request"]

from collections import UserString
from collections.abc import Buffer, Callable, Iterable, Mapping
from copy import copy
from http.cookiejar import CookieJar
from http.cookies import BaseCookie
from inspect import signature
from os import PathLike
from types import EllipsisType
from typing import cast, overload, Any, Final, Literal

from cookietools import extract_cookies, update_cookies
from dicttools import get_all_items
from filewrap import bio_chunk_iter, SupportsRead
from http_request import normalize_request_args, SupportsGeturl
from http_response import parse_response, get_length
from requests import adapters
from requests.cookies import RequestsCookieJar
from requests.models import Request, Response
from requests.sessions import Session
from yarl import URL


type string = Buffer | str | UserString

_BUILD_REQUEST_KWARGS: Final = signature(Request).parameters.keys()
_MERGE_SETTING_KWARGS: Final = signature(Session.merge_environment_settings).parameters.keys() - {"self", "url"}
_SEND_REQUEST_KWARGS: Final  = signature(adapters.HTTPAdapter.send).parameters.keys() - {"self", "request"} | {"allow_redirects"}

if "__del__" not in Response.__dict__:
    setattr(Response, "__del__", Response.close)
if "__del__" not in Session.__dict__:
    setattr(Session, "__del__", Session.close)

adapters.DEFAULT_RETRIES = 5
_DEFAULT_SESSION = Session()


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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Session = _DEFAULT_SESSION, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
) -> Response:
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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Session = _DEFAULT_SESSION, 
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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Session = _DEFAULT_SESSION, 
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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Session = _DEFAULT_SESSION, 
    *, 
    parse: Callable[[Response, bytes], T], 
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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Session = _DEFAULT_SESSION, 
    *, 
    parse: None | EllipsisType| bool | Callable[[Response, bytes], T] = None, 
    **request_kwargs, 
) -> Response | bytes | str | dict | list | int | float | bool | None | T:
    request_kwargs["allow_redirects"] = follow_redirects
    request_kwargs["stream"] = stream
    if session is None:
        session = Session()
    if cookies is not None:
        if isinstance(cookies, RequestsCookieJar):
            request_kwargs["cookies"] = cookies
        else:
            request_kwargs["cookies"] = update_cookies(RequestsCookieJar(), cookies)
    if isinstance(url, Request):
        request = url
        if cookies is not None:
            request = copy(request)
            request.cookies = cookies
    else:
        if isinstance(data, PathLike):
            data = bio_chunk_iter(open(data, "rb"))
        elif isinstance(data, SupportsRead):
            data = bio_chunk_iter(data)
        request_kwargs.update(normalize_request_args(
            method=method, 
            url=url, 
            params=params, 
            data=data, 
            json=json, 
            files=files, 
            headers=headers, 
        ))
        request = Request(**dict(get_all_items(
            request_kwargs, *_BUILD_REQUEST_KWARGS)))
    prep = session.prepare_request(request)
    request_kwargs.setdefault("proxies", {})
    request_kwargs.setdefault("verify", session.verify)
    request_kwargs.setdefault("cert", session.cert)
    request_kwargs.update(session.merge_environment_settings(
        prep.url, **dict(get_all_items(request_kwargs, *_MERGE_SETTING_KWARGS))))
    response = session.send(
        prep, **dict(get_all_items(request_kwargs, *_SEND_REQUEST_KWARGS)))
    setattr(response, "session", session)
    if cookies is not None:
        try:
            if response.cookies:
                update_cookies(cookies, response.cookies) # type: ignore
        except TypeError:
            from http.client import HTTPMessage
            response_headers = HTTPMessage()
            for key, val in response.headers.items():
                response_headers.set_raw(key, val)
            setattr(response, "info", lambda: response_headers)
            extract_cookies(cookies, response.url, response) # type: ignore
    if raise_for_status:
        response.raise_for_status()
    if parse is None:
        if method == "HEAD":
            response.content
        return response
    elif parse is ...:
        try:
            if (method == "HEAD" or 
                (length := get_length(response)) is not None and length <= 10485760
            ):
                response.content
        finally:
            response.close()
        return response
    content = response.content
    if isinstance(parse, bool):
        if not parse:
            return content
        parse = cast(Callable, parse_response)
    return parse(response, content)

