#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 3)
__all__ = ["startfile", "startfile_async"]

try:
    from os import startfile # type: ignore
except ImportError:
    from asyncio import create_subprocess_exec, create_subprocess_shell
    from platform import system
    from subprocess import run

    async def run_command(command):
        if isinstance(command, str):
            process = await create_subprocess_shell(command)
        else:
            process = await create_subprocess_exec(*command)
        await process.communicate()

    match system():
        case "Linux" | "Android":
            commnd = "xdg-open"
        case "Darwin":
            commnd = "open"
        case "Windows":
            commnd = "start"
        case _:
            from shutil import which
            if which("xdg-open"):
                commnd = "xdg-open"
            else:
                raise RuntimeError("can't get startfile")
    def startfile(path, /, *args):
        run([commnd, path, *args])
    async def startfile_async(path, /, *args):
        await run_command([commnd, path, *args])
else:
    from asyncio import to_thread
    from functools import wraps

    @wraps(startfile)
    async def startfile_async(*args, **kwds):
        return await to_thread(startfile, *args, **kwds)

