#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 1, 0)
__all__ = ["request"]

from collections import UserString
from collections.abc import Buffer, Callable, Iterable, Mapping
from inspect import signature
from os import PathLike
from types import EllipsisType
from typing import cast, overload, Any, Final, Literal

from argtools import argcount
from dicttools import get_all_items
from ensure import ensure_buffer
from filewrap import bio_chunk_iter, SupportsRead
from http_request import normalize_request_args, SupportsGeturl
from http_response import parse_response
from requests import adapters
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

_DEFAULT_SESSION = Session()
adapters.DEFAULT_RETRIES = 5


@overload
def request(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    stream: bool = True, 
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
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    stream: bool = True, 
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
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    stream: bool = True, 
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
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    stream: bool = True, 
    session: None | Session = _DEFAULT_SESSION, 
    *, 
    parse: Callable[[Response, bytes], T] | Callable[[Response], T], 
    **request_kwargs, 
) -> T:
    ...
def request[T](
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    stream: bool = True, 
    session: None | Session = _DEFAULT_SESSION, 
    *, 
    parse: None | EllipsisType| bool | Callable[[Response, bytes], T] | Callable[[Response], T] = None, 
    **request_kwargs, 
) -> Response | bytes | str | dict | list | int | float | bool | None | T:
    request_kwargs["allow_redirects"] = follow_redirects
    request_kwargs["stream"] = stream
    if session is None:
        session = Session()
    if isinstance(url, Request):
        request = url
    else:
        if isinstance(data, PathLike):
            data = bio_chunk_iter(open(data, "rb"))
        elif isinstance(data, SupportsRead):
            data = map(ensure_buffer, bio_chunk_iter(data))
        request_kwargs.update(normalize_request_args(
            method=method, 
            url=url, 
            params=params, 
            data=data, 
            json=json, 
            headers=headers, 
        ))
        request = Request(**dict(get_all_items(
            request_kwargs, *_BUILD_REQUEST_KWARGS)))
    prep = session.prepare_request(request)
    request_kwargs.setdefault("proxies", None)
    request_kwargs.setdefault("verify", session.verify)
    request_kwargs.setdefault("cert", session.cert)
    request_kwargs.update(session.merge_environment_settings(
        prep.url, **dict(get_all_items(request_kwargs, *_MERGE_SETTING_KWARGS))))
    response = session.send(
        prep, **dict(get_all_items(request_kwargs, *_SEND_REQUEST_KWARGS)))
    setattr(response, "session", session)
    if raise_for_status:
        response.raise_for_status()
    if parse is None:
        return response
    elif parse is ...:
        response.close()
        return response
    with response:
        if isinstance(parse, bool):
            content = response.content
            if parse:
                return parse_response(response, content)
            return content
        ac = argcount(parse)
        with response:
            if ac == 1:
                return cast(Callable[[Response], T], parse)(response)
            else:
                return cast(Callable[[Response, bytes], T], parse)(
                    response, response.content)

