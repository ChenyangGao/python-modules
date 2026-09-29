#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__doc__ = "目录树信息遍历导出"

from argparse import ArgumentParser, Namespace, RawTextHelpFormatter
from hashlib import algorithms_available

KEYS = ["inode", "name", "path", "relpath", "is_dir", "stat", "stat_info"]

parser = ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)

parser.add_argument("path", nargs="?", default=".", help="文件夹路径，默认为当前工作目录")
parser.add_argument("-m", "--min-depth", default=0, type=int, help="最小深度，默认值 0，小于 0 时不限")
parser.add_argument("-M", "--max-depth", default=-1, type=int, help="最大深度，默认值 -1，小于 0 时不限")
parser.add_argument("-k", "--keys", choices=KEYS, nargs="*", help=f"选择输出的 key，默认输出所有可选值")
parser.add_argument("-s", "--select", default="", help="对路径进行筛选，提供一个表达式（会注入一个变量 entry，类型是 iterdir.DirEntry）或函数（会传入一个参数，类型是 iterdir.DirEntry）")
parser.add_argument("-se", "--select-exec", action="store_true", help="对 -s/--select 传入的代码用 exec 运行，其中必须存在名为 select 的函数。否则，视为表达式或 lambda 函数")
parser.add_argument("-o", "--output-file", help="""保存到文件，此时命令行会输出进度条，根据扩展名来决定输出格式
- *.csv   输出一个 csv，第 1 行为表头，以后每行输出一条数据
- *.json  输出一个 JSON Object 的列表
- *       每行输出一条 JSON Object
""")
parser.add_argument("-hs", "--hashes", choices=(*algorithms_available, "crc32"), nargs="*", help="计算文件的哈希值，可以选择多个算法")
parser.add_argument("-dfs", "--depth-first", action="store_true", help="使用深度优先搜索，否则使用广度优先")
parser.add_argument("-fl", "--follow-symlinks", action="store_true", help="跟进符号连接，否则会把符号链接视为文件，即使它指向目录")
parser.add_argument("-v", "--version", action="store_true", help="输出版本号")

from binascii import crc32
from collections.abc import Callable, Iterator, Sequence
from functools import partial
from hashlib import new as hashnew
from os import fsdecode, PathLike
from os.path import abspath, isdir, islink, relpath
from sys import stdout
from textwrap import dedent
from typing import cast, Any, TextIO


def file_multi_hashes(
    path: bytes | str | PathLike, 
    hashes: Sequence[str], 
) -> None | dict[str, str]:
    try:
        file = open(path, "rb", buffering=0)
    except OSError:
        return None
    cache: dict[str, Any] = {alg: 0 if alg == "crc32" else hashnew(alg) for alg in hashes}
    updates = tuple(
        (lambda data: 
            cache.__setitem__("crc32", crc32(data, cast(int, cache["crc32"])))
        ) if alg == "crc32" else val.update 
        for alg, val in cache.items()
    )
    readinto = file.readinto
    buf = bytearray(1 << 16) # 64 KB
    view = memoryview(buf)
    while (size := readinto(buf)):
        for update in updates:
            update(view[:size])
    return {alg: format(val, "x") if alg == "crc32" else val.hexdigest() for alg, val in cache.items()}


def parse_args(argv: None | list[str] = None, /) -> Namespace:
    args = parser.parse_args(argv)
    if args.version:
        from iterdir import __version__
        print(".".join(map(str, __version__)))
        raise SystemExit(0)
    return args


def main(argv: None | list[str] | Namespace = None, /):
    if isinstance(argv, Namespace):
        args = argv
    else:
        args = parse_args(argv)

    from iterdir import iterdir, DirEntry

    predicate: None | Callable[[DirEntry], None | bool] = None
    if select_code := dedent(args.select).strip():
        ns: dict = {"re": __import__("re")}
        if args.select_exec:
            exec(select_code, ns)
            predicate = ns.get("select")
        elif select_code.startswith("lambda "):
            predicate = eval(select_code, ns)
        else:
            predicate = eval("lambda entry:" + select_code, ns)

    follow_symlinks = args.follow_symlinks
    path_it: Iterator[DirEntry] = iterdir(
        args.path, 
        topdown=True if args.depth_first else None, 
        min_depth=args.min_depth, 
        max_depth=args.max_depth, 
        predicate=predicate, 
        follow_symlinks=follow_symlinks, 
    )

    output_file = args.output_file
    if output_file:
        from collections import deque
        from time import perf_counter
        from texttools import format_time

        def progress(it):
            write = stdout.buffer.raw.write # type: ignore
            dq: deque[tuple[int, float]] = deque(maxlen=10*60)
            push = dq.append
            total = 0
            ndirs = 0
            nfiles = 0
            start_t = last_t = perf_counter()
            write(f"\r\x1b[K🗂️  {total} = 📂 {ndirs} + 📝 {nfiles}".encode())
            push((total, start_t))
            for p in it:
                total += 1
                if p.is_dir():
                    ndirs += 1
                else:
                    nfiles += 1
                cur_t = perf_counter()
                if cur_t - last_t > 0.1:
                    speed = (total - dq[0][0]) / (cur_t - dq[0][1])
                    write(f"\r\x1b[K🗂️  {total} = 📂 {ndirs} + 📝 {nfiles} | 🕙 {format_time(cur_t-start_t)} | 🚀 {speed:.3f} it/s".encode())
                    push((total, cur_t))
                    last_t = cur_t
                yield p
            cur_t = perf_counter()
            speed = total / (cur_t - start_t)
            write(f"\r\x1b[K🗂️  {total} = 📂 {ndirs} + 📝 {nfiles} | 🕙 {format_time(cur_t-start_t)} | 🚀 {speed:.3f} it/s".encode())
        file: TextIO = open(output_file, "w")
        path_it = iter(progress(path_it))
        if output_file.endswith(".csv"):
            output_type = "csv"
        elif output_file.endswith(".json"):
            output_type = "json"
        else:
            output_type = "log"
    else:
        file = stdout
        output_type = "log"
    write = file.buffer.write
    if output_type in ("log", "json"):
        dumps: Callable[..., bytes]
        try:
            from orjson import dumps
        except ImportError:
            from json import dumps as _dumps
            dumps = lambda o, /: _dumps(o, ensure_ascii=False).encode("utf-8")

    fmap: dict[str, Callable] = {
        "inode": DirEntry.inode, 
        "name": lambda e: e.name, 
        "path": lambda e: e.path, 
        "relpath": lambda e, start=abspath(args.path): relpath(abspath(e), start), 
        "is_dir": lambda e: e.is_dir(follow_symlinks=follow_symlinks), 
        "stat": lambda e: e.stat_dict(follow_symlinks=follow_symlinks, with_st=True), 
        "stat_info": lambda e: e.stat_info(follow_symlinks=follow_symlinks), 
    }

    keys: list[str] = args.keys
    if keys:
        fmap = {k: fmap[k] for k in keys if k in fmap}
        keys = list(fmap)
    else:
        keys = KEYS

    if args.hashes:
        keys.append("hashes")
        fmap["hashes"] = partial(file_multi_hashes, hashes=args.hashes)

    try:
        records = ({k: f(e) for k, f in fmap.items()} for e in path_it)
        if output_type == "json":
            write(b"[")
            for i, record in enumerate(records):
                if i:
                    write(b", ")
                write(dumps(record))
            write(b"]")
        elif output_type == "log":
            for record in records:
                write(dumps(record))
                write(b"\n")
        else:
            from csv import DictWriter

            writer = DictWriter(file, fieldnames=keys)
            writer.writeheader()
            for record in records:
                writer.writerow(record)
    except KeyboardInterrupt:
        pass
    except BrokenPipeError:
        from sys import stderr
        stderr.close()
    finally:
        file.close()


if __name__ == "__main__":
    from pathlib import Path
    from sys import path

    path[0] = str(Path(__file__).parents[1])
    main()

