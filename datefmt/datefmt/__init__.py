#!/usr/bin/env python3
# coding: utf-8

"""Utilities for converting between Unix timestamps and standard date string formats, 
specifically supporting ISO 8601 (local timezone) and RFC 5322/2822 GMT formats.
"""

__author__  = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 1)
__all__ = [
    "timestamp2isoformat", "isoformat2timestamp", 
    "timestamp2gmtformat", "gmtformat2timestamp", 
]

from calendar import timegm
from datetime import datetime
from email.utils import formatdate, parsedate
from time import time
from typing import cast


def timestamp2isoformat(ts: None | float | datetime = None, /) -> str:
    """
    Converts a Unix **timestamp** (seconds since the epoch) to an 
    **ISO 8601** (or RFC 3339) formatted string, including timezone information.

    The conversion is done by creating a timezone-aware datetime object
    from the timestamp using the system's local timezone.

    >>> timestamp = 1762921070
    >>> timestamp2isoformat(timestamp)
    '2025-11-12T12:17:50+08:00'
    >>> from datetime import datetime
    >>> now = datetime.now().astimezone()
    >>> now.strftime("%FT%X.%f%:z") == timestamp2isoformat(now)
    True

    Args:
        ts: The Unix timestamp to convert.

    Returns:
        A timezone-aware string representing the date and time 
        in ISO 8601 format (e.g., 'YYYY-MM-DDTHH:MM:SS.ffffff+00:00').

    References:
        - ISO 8601: https://en.wikipedia.org/wiki/ISO_8601
        - RFC 3339: https://datatracker.ietf.org/doc/html/rfc3339#section-5.6
    """
    if ts is None:
        dt = datetime.now()
    elif isinstance(ts, datetime):
        dt = ts
    else:
        dt = datetime.fromtimestamp(ts)
    return dt.astimezone().isoformat()


def isoformat2timestamp(s: str, /) -> float:
    """
    Converts an **ISO 8601** (or RFC 3339) formatted string to a Unix **timestamp** (seconds since the epoch).

    The string must be a valid ISO 8601 format (e.g., 'YYYY-MM-DDTHH:MM:SS' 
    or 'YYYY-MM-DDTHH:MM:SS+00:00').

    >>> isoformat = '2025-11-12T12:17:50+08:00'
    >>> isoformat2timestamp(isoformat)
    1762921070.0
    >>> from time import time
    >>> now = int(time())
    >>> isoformat2timestamp(timestamp2isoformat(now)) == now
    True

    Args:
        s: The ISO 8601 formatted string to convert.

    Returns:
        The Unix timestamp corresponding to the date and time.

    References:
        - ISO 8601: https://en.wikipedia.org/wiki/ISO_8601
        - RFC 3339: https://datatracker.ietf.org/doc/html/rfc3339#section-5.6
    """
    return datetime.fromisoformat(s).timestamp()


def timestamp2gmtformat(ts: None | float | datetime = None, /) -> str:
    """
    Converts a Unix **timestamp** (seconds since the epoch) to an **RFC 5322** (or RFC 2822) compliant date string, formatted specifically as **GMT**.

    This format is commonly used for HTTP headers (like `Date`) and email 
    headers.

    >>> timestamp = 1762921070
    >>> timestamp2gmtformat(timestamp)
    'Wed, 12 Nov 2025 04:17:50 GMT'
    >>> from datetime import datetime, timezone
    >>> now_utc = datetime.now(timezone.utc)
    >>> now_utc.strftime("%a, %d %b %Y %H:%M:%S GMT") == timestamp2gmtformat(now_utc)
    True

    Args:
        ts: The Unix timestamp to convert.

    Returns:
        A string representing the date and time in RFC 5322 GMT format 
        (e.g., 'Wed, 12 Nov 2025 03:54:09 GMT').

    References:
        - RFC 2822 (obsoleted by RFC 5322): https://datatracker.ietf.org/doc/html/rfc2822#section-3.3
        - RFC 5322: https://datatracker.ietf.org/doc/html/rfc5322#section-3.3
    """
    if ts is None:
        ts = time()
    elif isinstance(ts, datetime):
        ts = ts.timestamp()
    return formatdate(ts, usegmt=True)


def gmtformat2timestamp(s: str, /) -> int:
    """
    Converts an **RFC 5322** (or RFC 2822) compliant date string to a Unix 
    **timestamp** (seconds since the epoch).

    This function attempts to parse various valid formats defined by the RFCs.

    >>> isoformat = 'Wed, 12 Nov 2025 04:17:50 GMT'
    >>> gmtformat2timestamp(isoformat)
    1762921070
    >>> from time import time
    >>> now = int(time())
    >>> gmtformat2timestamp(timestamp2gmtformat(now)) == now
    True

    Args:
        s: The RFC 5322 formatted string (e.g., 'Wed, 12 Nov 2025 03:54:09 GMT').

    Returns:
        The Unix timestamp corresponding to the parsed date and time.

    References:
        - RFC 2822 (obsoleted by RFC 5322): https://datatracker.ietf.org/doc/html/rfc2822#section-3.3
        - RFC 5322: https://datatracker.ietf.org/doc/html/rfc5322#section-3.3
    """
    return timegm(cast(tuple[int, ...], parsedate(s)))


if __name__ == "__main__":
    import doctest
    doctest.testmod()

