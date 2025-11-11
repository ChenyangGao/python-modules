#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 1)
__all__ = ["socket_keepalive_linux", "socket_keepalive_osx", "socket_keepalive_win", "socket_keepalive"]

import socket

from platform import system
from socket import socket as Socket


def socket_keepalive_linux(
    sock: Socket, 
    after_idle_sec: int = 1, 
    interval_sec: int = 5, 
    max_fails: int = 5, 
):
    """Set TCP keepalive on an open socket on Linux.
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, after_idle_sec) # type: ignore
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)


def socket_keepalive_osx(
    sock: Socket, 
    after_idle_sec: int = 1, 
    interval_sec: int = 5, 
    max_fails: int = 5, 
):
    """Set TCP keepalive on an open socket on MaxOSX.
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, after_idle_sec) # type: ignore
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)


def socket_keepalive_win(
    sock: Socket, 
    after_idle_sec: int = 1, 
    interval_sec: int = 5, 
    max_fails: int = 5, 
):
    """Set TCP keepalive on an open socket on Windows.
    """
    sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, after_idle_sec * 1000, interval_sec * 1000)) # type: ignore


def socket_keepalive(
    sock: Socket, 
    after_idle_sec: int = 1, 
    interval_sec: int = 3, 
    max_fails: int = 5, 
):
    """Set TCP keepalive on an open socket on multiple platforms.

    It activates after `after_idle_sec` second of idleness,
    then sends a keepalive ping once every `interval_sec` seconds,
    and closes the connection after `max_fails` failed ping (max_fails), or 15 seconds.
    """
    platform = system()
    match platform:
        case "Darwin":
            return socket_keepalive_osx(sock, after_idle_sec, interval_sec, max_fails)
        case "Windows":
            return socket_keepalive_win(sock, after_idle_sec, interval_sec, max_fails)
        case "Linux":
            return socket_keepalive_linux(sock, after_idle_sec, interval_sec, max_fails)
    raise RuntimeError(f"unsupport platform {platform!r}")

