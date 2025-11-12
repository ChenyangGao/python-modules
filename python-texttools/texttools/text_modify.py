#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__all__ = ["prefix", "suffix"]


def prefix[AnyStr: (bytes, str)](s: AnyStr, /, prefix: AnyStr) -> AnyStr:
    if not prefix or s.startswith(prefix):
        return s
    return prefix + s


def suffix[AnyStr: (bytes, str)](s: AnyStr, /, suffix: AnyStr) -> AnyStr:
    if not suffix or s.endswith(suffix):
        return s
    return s + suffix

