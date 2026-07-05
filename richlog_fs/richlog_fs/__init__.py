#!/usr/bin/env python3
# encoding: utf-8

from __future__ import annotations

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 3)
__all__ = ["ColoredLevelNameFormatter", "get_logger", "access_log"]

import logging

from collections.abc import Callable
from functools import update_wrapper
from io import text_encoding
from itertools import chain
from textwrap import indent
from time import time
from traceback import format_exc
from typing import Literal

from errno2 import errno
from decotools import optional
from rich.console import Console


class missingdict[K, V](dict[K, V]):

    def __init__(self, missing: Callable[[K], V], /, *args, **kwds):
        self.missing = missing
        super().__init__(*args, **kwds)

    def __missing__(self, key: K, /) -> V:
        return self.missing(key)


class ColoredLevelNameFormatter(logging.Formatter):

    def format(self, record):
        match record.levelno:
            case logging.DEBUG:
                # bold blue
                record.levelname = f"\x1b[1;34m{record.levelname}\x1b[0m"
            case logging.INFO:
                # bold green
                record.levelname = f"\x1b[1;32m{record.levelname}\x1b[0m"
            case logging.WARNING:
                # bold yellow
                record.levelname = f"\x1b[1;33m{record.levelname}\x1b[0m"
            case logging.ERROR:
                # bold red
                record.levelname = f"\x1b[1;31m{record.levelname}\x1b[0m"
            case logging.CRITICAL:
                # bold magenta
                record.levelname = f"\x1b[1;35m{record.levelname}\x1b[0m"
            case _:
                # bold dim
                record.levelname = f"\x1b[1;2m{record.levelname}\x1b[0m"
        return super().format(record)

    @classmethod
    def new(
        cls, 
        /, 
        fmt: str = "", 
        style: Literal["%", "{", "$"] = "%", 
        **kwargs, 
    ) -> ColoredLevelNameFormatter:
        if not fmt:
            fmt = "[\x1b[1m%(asctime)s\x1b[0m] \x1b[1;36m%(name)s\x1b[0m(%(levelname)s) \x1b[5;31m➜\x1b[0m %(message)s"
            if style == "{":
                fmt %= missingdict("{%s}".__mod__)
            elif style == "$":
                fmt %= missingdict("${%s}".__mod__)
        return cls(fmt, style=style, **kwargs)

    @classmethod
    def get_logger(
        cls, 
        /, 
        name: str = "root", 
        force: bool = False, 
        encoding: None | str = "encoding", 
        errors: None | str = "backslashreplace", 
        level: None | int = logging.DEBUG if __debug__ else logging.NOTSET, 
        **kwargs, 
    ) -> logging.Logger:
        logger = logging.getLogger(name)
        if force:
            for h in logger.handlers[:]:
                logger.removeHandler(h)
                h.close()
        if not logger.handlers:
            handlers = kwargs.pop("handlers", None)
            if handlers is None:
                if "stream" in kwargs and "filename" in kwargs:
                    raise ValueError("'stream' and 'filename' should not be "
                                     "specified together")
            elif "stream" in kwargs or "filename" in kwargs:
                raise ValueError("'stream' or 'filename' should not be "
                                 "specified together with 'handlers'")
            if handlers is None:
                filename = kwargs.pop("filename", None)
                mode = kwargs.pop("filemode", 'a')
                if filename:
                    if 'b' in mode:
                        errors = None
                    else:
                        encoding = text_encoding(encoding)
                    h = logging.FileHandler(filename, mode, encoding=encoding, errors=errors)
                else:
                    stream = kwargs.pop("stream", None)
                    h = logging.StreamHandler(stream)
                handlers = [h]
            fmt = cls.new(
                kwargs.pop("format", ""), 
                datefmt=kwargs.pop("datefmt", None), 
                style=kwargs.pop("style", '%'), 
            )
            for h in handlers:
                if h.formatter is None:
                    h.setFormatter(fmt)
                logger.addHandler(h)
            if level is not None:
                logger.setLevel(level)
            if kwargs:
                keys = ', '.join(kwargs.keys())
                raise ValueError('Unrecognised argument(s): %s' % keys)
        return logger


def rich_format(*args, markup: bool = False, **kwds) -> str:
    console = Console()
    with console.capture() as capture:
        console.print(*args, markup=markup, **kwds)
    return capture.get().removesuffix("\n")


get_logger = ColoredLevelNameFormatter.get_logger


@optional
def access_log[**Args, R](
    func: Callable[Args, R], 
    /, 
    logger = "root", 
    level: None | int = logging.DEBUG if __debug__ else logging.NOTSET, 
) -> Callable[Args, R]:
    def to_call_str(args: tuple = (), kwds: dict = {}) -> str:
        return ", ".join(chain(map(repr, args), map("%s=%r".__mod__, kwds.items())))
    if isinstance(logger, str):
        logger = ColoredLevelNameFormatter.get_logger(logger, level=level)
    tpl = f"{getattr(func, '__qualname__', None) or getattr(func, '__name__')}(%s)"
    debug = logger.debug
    error = logger.error
    if level is None:
        level = logger.level
    def wrapper(*args: Args.args, **kwds: Args.kwargs) -> R:
        sig = tpl % to_call_str(args, kwds)
        start_t = time()
        try:
            r = func(*args, **kwds)
        except BaseException as e:
            if level == logging.DEBUG:
                errmsg = rich_format(indent(format_exc().strip(), "    ├ "))
            else:
                errmsg = rich_format(indent(f"[bold magenta]{type(e).__qualname__}[/bold magenta]: {e}", "    ├ "), markup=True)
            error(rich_format(f"{sig} {(time()-start_t) * 1000:.0f} ms\n" + errmsg))
            if isinstance(e, OSError):
                raise
            raise OSError(errno.EIO, "") from e
        else:
            if level == logging.DEBUG:
                debug(rich_format(f"{sig} {(time()-start_t) * 1000:.0f} ms"))
            return r
    return update_wrapper(wrapper, func)

