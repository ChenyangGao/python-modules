#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 2)
__all__ = ["middleware_access_log"]
__license__ = "GPLv3 <https://www.gnu.org/licenses/gpl-3.0.txt>"

import logging

from collections.abc import Buffer, Callable
from http import HTTPStatus
from io import UnsupportedOperation
from textwrap import indent
from time import time
from traceback import format_exc
from urllib.parse import unquote, urlsplit, urlunsplit

from blacksheep import text, Application, Request, Response
from blacksheep.exceptions import HTTPException
from orjson import dumps, OPT_INDENT_2, OPT_SORT_KEYS
from rich.box import ROUNDED
from rich.console import Console
from rich.highlighter import JSONHighlighter
from rich.panel import Panel
from rich.text import Text


class ColoredLevelNameFormatter(logging.Formatter):

    def format(self, record, /):
        match record.levelno:
            case logging.DEBUG:
                # blue
                record.levelname = f"\x1b[34m{record.levelname}\x1b[0m:".ljust(18)
            case logging.INFO:
                # green
                record.levelname = f"\x1b[32m{record.levelname}\x1b[0m:".ljust(18)
            case logging.WARNING:
                # yellow
                record.levelname = f"\x1b[33m{record.levelname}\x1b[0m:".ljust(18)
            case logging.ERROR:
                # red
                record.levelname = f"\x1b[31m{record.levelname}\x1b[0m:".ljust(18)
            case logging.CRITICAL:
                # magenta
                record.levelname = f"\x1b[35m{record.levelname}\x1b[0m:".ljust(18)
            case _:
                # dark grey
                record.levelname = f"\x1b[2m{record.levelname}\x1b[0m: ".ljust(18)
        return super().format(record)


def highlight_json(val, /, default=repr, highlighter=JSONHighlighter()) -> Text:
    if isinstance(val, Buffer):
        val = str(val, "utf-8")
    if not isinstance(val, str):
        val = dumps(val, default=default, option=OPT_INDENT_2 | OPT_SORT_KEYS).decode("utf-8")
    return highlighter(val)


def middleware_access_log(
    app: Application, 
    /, 
    redirect_exception_response: None | Callable[[Exception], Response] = None, 
):
    if redirect_exception_response is None:
        def redirect_exception_response(exc: Exception, /) -> Response:
            if isinstance(exc, HTTPException):
                return text(str(exc), exc.status)
            elif isinstance(exc, (ValueError, NotADirectoryError, IsADirectoryError)):
                return text(str(exc), 400)
            elif isinstance(exc, PermissionError):
                return text(str(exc), 403)
            elif isinstance(exc, FileNotFoundError):
                return text(str(exc), 404)
            elif isinstance(exc, UnsupportedOperation):
                return text(str(exc), 405)
            elif isinstance(exc, TimeoutError):
                return text(str(exc), 408)
            elif isinstance(exc, FileExistsError):
                return text(str(exc), 409)
            elif isinstance(exc, NotImplementedError):
                return text(str(exc), 501)
            elif isinstance(exc, OSError):
                return text(str(exc), 503)
            else:
                return text(str(exc), 500)

    logger = getattr(app, "logger")
    handler = logging.StreamHandler()
    handler.setFormatter(ColoredLevelNameFormatter("[\x1b[1m%(asctime)s\x1b[0m] %(levelname)s %(message)s"))
    logger.addHandler(handler)
    show_error_details = app.show_error_details

    @app.middlewares.append
    async def access_log(request: Request, handler: Callable, /) -> Response:
        start_t = time()
        error_msg = ""
        try:
            response = await handler(request)
            level = logging.INFO
        except Exception as e:
            level = logging.ERROR
            response = redirect_exception_response(e)
            if show_error_details:
                console = Console()
                with console.capture() as capture:
                    console.print(indent(format_exc().strip(), "    ├ "))
                error_msg = "\n" + capture.get()
            else:
                exc_type = type(e)
                exc_name = exc_type.__qualname__
                exc_module = exc_type.__module__
                if exc_module not in ("__builtins__", "__main__"):
                    exc_name = exc_module + "." + exc_name
                error_msg = f"\n    |_ \x1b[1;31m{exc_name}\x1b[0m: {e}"
        host, port = request.scope["client"]
        status = response.status
        if not status:
            status = 200
        if status < 300:
            status_color = 32
        elif status < 400:
            status_color = 33
        else:
            status_color = 31
        message = f'\x1b[5;35m{host}:{port}\x1b[0m - "\x1b[1;36m{request.method}\x1b[0m \x1b[1;4;34m{request.url}\x1b[0m \x1b[1mHTTP/{request.scope["http_version"]}\x1b[0m" - \x1b[{status_color}m{status} {HTTPStatus(status).phrase}\x1b[0m - \x1b[32m{(time() - start_t) * 1000:.3f}\x1b[0m \x1b[3mms\x1b[0m{error_msg}'
        if show_error_details:
            console = Console()
            with console.capture() as capture:
                urlp = urlsplit(str(request.url))
                url = urlunsplit(urlp._replace(path=unquote(urlp.path), scheme=request.scheme, netloc=request.host))
                console.print(
                    Panel.fit(
                        f"[b cyan]{request.method}[/] [u blue]{url}[/] [b]HTTP/[red]{request.scope["http_version"]}",
                        box=ROUNDED,
                        title="[b red]URL", 
                        border_style="cyan", 
                    ), 
                )
                headers = {str(k, 'latin-1'): str(v, 'latin-1') for k, v in request.headers}
                console.print(
                    Panel.fit(
                        highlight_json(headers), 
                        box=ROUNDED, 
                        title="[b red]HEADERS", 
                        border_style="cyan", 
                    )
                )
                scope = {k: v for k, v in request.scope.items() if k != "headers"}
                console.print(
                    Panel.fit(
                        highlight_json(scope), 
                        box=ROUNDED, 
                        title="[b red]SCOPE", 
                        border_style="cyan", 
                    )
                )
            message += "\n" + capture.get()
        logger.log(level, message)
        return response
    return access_log

