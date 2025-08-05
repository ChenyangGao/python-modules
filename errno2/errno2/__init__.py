#!/usr/bin/env python3
# encoding: utf-8

from __future__ import annotations

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__all__ = ["errno"]
__version__ = (0, 0, 1)

from enum import IntEnum


class errno(IntEnum):
    """errno — Standard errno system symbol.

    .. admonition:: Reference

        https://docs.python.org/3/library/errno.html
    """
    EPERM = 1
    ENOENT = 2
    ESRCH = 3
    EINTR = 4
    EIO = 5
    ENXIO = 6
    E2BIG = 7
    ENOEXEC = 8
    EBADF = 9
    ECHILD = 10
    EAGAIN = 11
    ENOMEM = 12
    EACCES = 13
    EFAULT = 14
    ENOTBLK = 15
    EBUSY = 16
    EEXIST = 17
    EXDEV = 18
    ENODEV = 19
    ENOTDIR = 20
    EISDIR = 21
    EINVAL = 22
    ENFILE = 23
    EMFILE = 24
    ENOTTY = 25
    ETXTBSY = 26
    EFBIG = 27
    ENOSPC = 28
    ESPIPE = 29
    EROFS = 30
    EMLINK = 31
    EPIPE = 32
    EDOM = 33
    ERANGE = 34
    EDEADLK = 35
    ENAMETOOLONG = 36
    ENOLCK = 37
    ENOSYS = 38
    ENOTEMPTY = 39
    ELOOP = 40
    EWOULDBLOCK = 41
    ENOMSG = 42
    EIDRM = 43
    ECHRNG = 44
    EL2NSYNC = 45
    EL3HLT = 46
    EL3RST = 47
    ELNRNG = 48
    EUNATCH = 49
    ENOCSI = 50
    EL2HLT = 51
    EBADE = 52
    EBADR = 53
    EXFULL = 54
    ENOANO = 55
    EBADRQC = 56
    EBADSLT = 57
    EDEADLOCK = 58
    EBFONT = 59
    ENOSTR = 60
    ENODATA = 61
    ETIME = 62
    ENOSR = 63
    ENONET = 64
    ENOPKG = 65
    EREMOTE = 66
    ENOLINK = 67
    EADV = 68
    ESRMNT = 69
    ECOMM = 70
    EPROTO = 71
    EMULTIHOP = 72
    EDOTDOT = 73
    EBADMSG = 74
    EOVERFLOW = 75
    ENOTUNIQ = 76
    EBADFD = 77
    EREMCHG = 78
    ELIBACC = 79
    ELIBBAD = 80
    ELIBSCN = 81
    ELIBMAX = 82
    ELIBEXEC = 83
    EILSEQ = 84
    ERESTART = 85
    ESTRPIPE = 86
    EUSERS = 87
    ENOTSOCK = 88
    EDESTADDRREQ = 89
    EMSGSIZE = 90
    EPROTOTYPE = 91
    ENOPROTOOPT = 92
    EPROTONOSUPPORT = 93
    ESOCKTNOSUPPORT = 94
    EOPNOTSUPP = 95
    ENOTSUP = 96
    EPFNOSUPPORT = 97
    EAFNOSUPPORT = 98
    EADDRINUSE = 99
    EADDRNOTAVAIL = 100
    ENETDOWN = 101
    ENETUNREACH = 102
    ENETRESET = 103
    ECONNABORTED = 104
    ECONNRESET = 105
    ENOBUFS = 106
    EISCONN = 107
    ENOTCONN = 108
    ESHUTDOWN = 109
    ETOOMANYREFS = 110
    ETIMEDOUT = 111
    ECONNREFUSED = 112
    EHOSTDOWN = 113
    EHOSTUNREACH = 114
    EALREADY = 115
    EINPROGRESS = 116
    ESTALE = 117
    EUCLEAN = 118
    ENOTNAM = 119
    ENAVAIL = 120
    EISNAM = 121
    EREMOTEIO = 122
    EDQUOT = 123
    EQFULL = 124
    ENOMEDIUM = 125
    EMEDIUMTYPE = 126
    ENOKEY = 127
    EKEYEXPIRED = 128
    EKEYREVOKED = 129
    EKEYREJECTED = 130
    ERFKILL = 131
    ELOCKUNMAPPED = 132
    ENOTACTIVE = 133
    EAUTH = 134
    EBADARCH = 135
    EBADEXEC = 136
    EBADMACHO = 137
    EDEVERR = 138
    EFTYPE = 139
    ENEEDAUTH = 140
    ENOATTR = 141
    ENOPOLICY = 142
    EPROCLIM = 143
    EPROCUNAVAIL = 144
    EPROGMISMATCH = 145
    EPROGUNAVAIL = 146
    EPWROFF = 147
    EBADRPC = 148
    ERPCMISMATCH = 149
    ESHLIBVERS = 150
    ENOTCAPABLE = 151
    ECANCELED = 152
    EOWNERDEAD = 153
    ENOTRECOVERABLE = 154

    def of(key: int | str | errno, /) -> errno:
        if isinstance(key, errno):
            return key
        if isinstance(key, int):
            return errno(key)
        try:
            return errno[key]
        except KeyError as e:
            raise ValueError(key) from e

