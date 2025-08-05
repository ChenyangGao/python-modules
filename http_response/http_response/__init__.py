#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 5)
__all__ = [
    "headers_get", "get_filename", "get_mimetype", "get_charset", 
    "get_content_length", "get_length", "get_total_length", 
    "get_range", "is_chunked", "is_range_request", "parse_response", 
]

from codecs import lookup
from collections.abc import Container, Iterable, Mapping
from mimetypes import guess_extension, guess_type
from posixpath import basename
from re import compile as re_compile, IGNORECASE
from typing import cast, Final
from urllib.parse import parse_qsl, urlsplit, unquote

from dicttools import get
from orjson import loads


CRE_CONTENT_RANGE_fullmatch: Final = re_compile(r"bytes\s+(?:\*|(?P<begin>[0-9]+)-(?P<end>[0-9]+))/(?:(?P<size>[0-9]+)|\*)").fullmatch
CRE_HDR_CD_FNAME_search: Final = re_compile("(?<=filename=\")[^\"]+|(?<=filename=')[^']+|(?<=filename=)[^'\"][^;]*").search
CRE_HDR_CD_FNAME_STAR_search: Final = re_compile("(?<=filename\\*=)(?P<charset>[^']*)''(?P<name>[^;]+)").search
CRE_CHARSET_search = re_compile(r"\bcharset\s*=(?P<charset>[^ ;]+)", IGNORECASE).search


def headers_get(response, /, key: bytes | str, default=None, parse=None):
    if hasattr(response, "getheaders"):
        headers = response.getheaders
    elif hasattr(response, "headers"):
        headers = response.headers
    else:
        headers = response
    headers = cast(Mapping | Iterable[tuple[bytes|str, bytes|str]], headers)
    key2: bytes | str
    if isinstance(headers, Mapping):
        val = get(headers, key, default=None)
        if val is None:
            if isinstance(key, str):
                key2 = bytes(key, "latin-1")
            else:
                key2 = str(key, "latin-1")
            val = get(headers, key2, default=None)
            if val is None:
                return default
    else:
        key = key.lower()
        if isinstance(key, str):
            key2 = bytes(key, "latin-1")
        else:
            key2 = str(key, "latin-1")
        for k, val in headers:
            k = k.lower()
            if k == key or k == key2:
                break
        else:
            return default
    if parse is None:
        return val
    elif callable(parse):
        return parse(val)
    elif isinstance(parse, (bytes, str)):
        return val == parse
    elif isinstance(parse, Mapping):
        return headers_get(parse, val, default)
    elif isinstance(parse, Container):
        return val in parse
    return val


def get_filename(response, /, default: str = "") -> str:
    # NOTE: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition
    if hdr_cd := headers_get(response, "content-disposition"):
        if match := CRE_HDR_CD_FNAME_STAR_search(hdr_cd):
            return unquote(match["name"], match["charset"] or "utf-8")
        if match := CRE_HDR_CD_FNAME_search(hdr_cd):
            return match[0]
    url = response.url
    if not isinstance(url, (bytearray, bytes, str)):
        url = str(url)
    urlp = urlsplit(url)
    for key, val in parse_qsl(urlp.query):
        if val and key.lower() in ("filename", "file_name"):
            return unquote(val)
    filename = basename(unquote(urlp.path)) or default
    if filename:
        if guess_type(filename)[0]:
            return filename
        if hdr_ct := headers_get(response, "content-type"):
            if idx := hdr_ct.find(";") > -1:
                hdr_ct = hdr_ct[:idx]
            ext = hdr_ct and guess_extension(hdr_ct) or ""
            if ext and not filename.endswith(ext, 1):
                filename += ext
    return filename


def get_mimetype(content_type: str, /) -> str:
    return content_type.strip(" ;").partition(";")[0].strip()


def get_charset(content_type: str, /, default="utf-8") -> str:
    if match := CRE_CHARSET_search(content_type):
        try:
            return lookup(match["charset"]).name
        except LookupError:
            return match["charset"].strip()
    return default


def get_content_length(response, /) -> None | int:
    if length := headers_get(response, "content-length"):
        return int(length)
    return None


def get_length(response, /) -> None | int:
    if (length := get_content_length(response)) is not None:
        return length
    if rng := get_range(response):
        return rng[1] - rng[0] + 1
    return None


def get_total_length(response, /) -> None | int:
    if rng := get_range(response):
        return rng[-1]
    return get_content_length(response)


def get_range(response, /) -> None | tuple[int, int, int]:
    hdr_cr = headers_get(response, "content-range")
    if not hdr_cr:
        return None
    if match := CRE_CONTENT_RANGE_fullmatch(hdr_cr):
        begin, end, size = match.groups()
        if begin:
            begin = int(begin)
            end = int(end)
            if size:
                size = int(size)
            else:
                size = end + 1
            return begin, end, size
        elif size:
            size = int(size)
            if size == 0:
                return 0, 0, 0
            return 0, size - 1, size
    return None


def is_chunked(response, /) -> bool:
    return headers_get(response, "transfer-encoding", default=False, parse="chunked")


def is_range_request(response, /) -> bool:
    return bool(headers_get(response, "accept-ranges", parse="bytes")
                or headers_get(response, "content-range"))


def parse_response(
    response, 
    content: bytes, 
    /, 
) -> bytes | str | dict | list | int | float | bool | None:
    content_type = headers_get(response, "content-type", default="")
    if not isinstance(content_type, str):
        content_type = str(content_type, "latin-1")
    mimetype = get_mimetype(content_type)
    charset  = get_charset(content_type)
    if mimetype == "application/json":
        if charset == "utf-8":
            return loads(content)
        else:
            return loads(content.decode(charset))
    elif content_type.startswith("text/"):
        return content.decode(charset)
    return content

