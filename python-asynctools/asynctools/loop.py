#!/usr/bin/env python3
# encoding: utf-8

__all__ = [
    "AsyncExecutor", "in_async", "get_loop", "stop_loop", 
    "shutdown_loop", "close_loop", "run_loop", "loop_submit", 
    "loop_run", "run_async", "iter_async"
]

from asyncio import (
    get_event_loop, get_running_loop, new_event_loop, run_coroutine_threadsafe, 
    set_event_loop, wait, AbstractEventLoop, Semaphore, Task, 
)
from asyncio.tasks import all_tasks
from collections.abc import AsyncIterable, Awaitable, Callable, Iterator
from concurrent.futures._base import Executor, Future
from contextvars import Context
from _thread import start_new_thread
from time import sleep

from .basic import ensure_coroutine


async def call_with_lock[**Args, R](
    lock, 
    func: Callable[Args, Awaitable[R]], 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
) -> R:
    if lock is None:
        return await func(*args, **kwds)
    async with lock:
        return await func(*args, **kwds)


class AsyncExecutor(Executor):

    def __init__(
        self, 
        max_workers: None | int = None, 
        loop: None | AbstractEventLoop = None, 
    ):
        self.loop = run_loop(loop, run_in_thread=True)
        if max_workers is None or max_workers <= 0:
            self.sema: None | Semaphore = None
        else:
            self.sema = Semaphore(max_workers)

    def __del__(self, /):
        self.shutdown(wait=False, cancel_futures=True)

    def __getattr__(self, attr, /):
        return getattr(self.loop, attr)

    def close(self, /, wait: bool = False):
        if wait:
            close_loop(self.loop)
        else:
            start_new_thread(close_loop, (self.loop,))

    def shutdown(self, /, wait: bool = True, cancel_futures: bool = False):
        if wait:
            loop = self.loop
            if not loop.is_closed():
                if cancel_futures:
                    while loop.is_running():
                        if tasks := all_tasks(loop):
                            for task in tasks:
                                task.cancel()
                        else:
                            loop.stop()
                        sleep(0.01)
                else:
                    while loop.is_running():
                        loop.stop()
                        sleep(0.01)
        else:
            start_new_thread(self.shutdown, (wait, cancel_futures))

    def submit[**Args, T]( # type: ignore
        self, 
        func: Callable[Args, Awaitable[T]], 
        /, 
        *args: Args.args, 
        **kwds: Args.kwargs, 
    ) -> Future[T]:
        if sema := self.sema:
            aw: Awaitable[T] = call_with_lock(sema, func, *args, **kwds)
        else:
            aw = func(*args, **kwds)
        return loop_submit(aw, self.loop)

    def create_task[T](
        self, 
        aw: Awaitable[T], 
        /, 
        name: None | str = None, 
        context: None | Context = None, 
    ) -> Task[T]:
        return self.loop.create_task(
            ensure_coroutine(aw), 
            name=name, 
            context=context, 
        )


def in_async() -> bool:
    try:
        get_running_loop()
        return True
    except RuntimeError:
        return False


def get_loop(
    set_loop: bool = False, 
    new_loop: Callable[[], AbstractEventLoop] = new_event_loop, 
) -> AbstractEventLoop:
    try:
        loop = get_event_loop()
        if loop.is_closed():
            raise RuntimeError
        return loop
    except RuntimeError:
        loop = new_loop()
        if set_loop:
            set_event_loop(loop)
        return loop


def stop_loop(loop: None | AbstractEventLoop = None):
    if loop is None:
        try:
            loop = get_event_loop()
        except RuntimeError:
            return
    loop.stop()


def shutdown_loop(loop: None | AbstractEventLoop = None):
    if loop is None:
        try:
            loop = get_event_loop()
        except RuntimeError:
            return
    if not loop.is_closed():
        if tasks := all_tasks(loop):
            for task in tasks:
                task.cancel()
            loop.run_until_complete(wait(tasks))
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.run_until_complete(loop.shutdown_default_executor())


def close_loop(
    loop: None | AbstractEventLoop = None, 
    sleep_interval: float = 0.01, 
):
    if loop is None:
        try:
            loop = get_event_loop()
        except RuntimeError:
            return
    if not loop.is_closed():
        while loop.is_running():
            if tasks := all_tasks(loop):
                for task in tasks:
                    task.cancel()
            else:
                loop.stop()
            sleep(sleep_interval)
        loop.close()


def run_loop(
    loop: None | AbstractEventLoop = None, 
    close_at_end: bool = False, 
    run_in_thread: bool = False, 
) -> AbstractEventLoop:
    if loop is None:
        loop = new_event_loop()
        close_at_end = True
    if run_in_thread:
        start_new_thread(run_loop, (loop, close_at_end))
    else:
        try:
            loop.run_forever()
        finally:
            if close_at_end:
                close_loop(loop)
    return loop


def loop_submit[T](
    aw: Awaitable[T], 
    /, 
    loop: None | AbstractEventLoop = None, 
) -> Future[T]:
    return run_coroutine_threadsafe(
        ensure_coroutine(aw), 
        loop or get_loop(True), 
    )


def loop_run[T](
    aw: Awaitable[T], 
    /, 
    loop: None | AbstractEventLoop = None, 
) -> T:
    if loop is None:
        loop = get_loop()
    try:
        return loop.run_until_complete(aw)
    finally:
        loop.close()


def run_async[T](
    o: Awaitable[T], 
    /, 
    loop: None | AbstractEventLoop = None, 
) -> T | Future[T]:
    if loop is None:
        loop = get_loop()
    if loop.is_running():
        return run_coroutine_threadsafe(ensure_coroutine(o), loop)
    else:
        return loop.run_until_complete(ensure_coroutine(o))


def iter_async[T](
    iterable: AsyncIterable[T], 
    /, 
    loop: None | AbstractEventLoop = None, 
) -> Iterator[T | Future[T]]:
    if loop is None:
        loop = get_loop()
    call = aiter(iterable).__anext__
    try:
        while True:
            yield run_async(call(), loop)
    except StopAsyncIteration:
        pass

