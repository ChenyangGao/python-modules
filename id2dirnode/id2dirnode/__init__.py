#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 1)
__all__ = ["IdToDirnode", "AncestorDict"]

from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, MutableMapping, Sequence, KeysView, ValuesView, ItemsView
from reprlib import recursive_repr
from typing import TypedDict
from weakref import WeakKeyDictionary


class AncestorDict(TypedDict):
    id: int
    parent_id: int
    name: str


class Str(str):
    "String class for making weak reference"


class IdToDirnode(MutableMapping[int, tuple[str, int]]):

    def __init__(self, data: Mapping[int, tuple[str, int]] | Iterable[tuple[int, tuple[str, int]]] = {}, /):
        self.data: dict[int, tuple[str, int]] = {}
        self._pid_to_children: defaultdict[int, WeakKeyDictionary[str, int]] = defaultdict(WeakKeyDictionary)
        if data:
            self.update(data)

    def __contains__(self, key, /) -> bool:
        try:
            return key in self.data
        except TypeError:
            return False

    @recursive_repr()
    def __repr__(self, /) -> str:
        cls = type(self)
        return f"{cls.__module__}.{cls.__qualname__}({self.data})"

    def __delitem__(self, cid: int, /):
        del self.data[cid]

    def __getitem__(self, cid: int, /) -> tuple[str, int]:
        return self.data[cid]

    def __setitem__(self, cid: int, pair: tuple[str, int], /):
        name, pid = pair
        name = Str(name)
        self.data[cid] = (name, pid)
        self._pid_to_children[pid][name] = cid

    def __iter__(self, /) -> Iterator[int]:
        return iter(self.data)

    def __len__(self, /) -> int:
        return len(self.data)

    def keys(self, /) -> KeysView[int]:
        return self.data.keys()

    def values(self, /) -> ValuesView[tuple[str, int]]:
        return self.data.values()

    def items(self, /) -> ItemsView[int, tuple[str, int]]:
        return self.data.items()

    def get_name(self, id: int, /) -> str:
        return str(self[id][0])

    def get_parent_id(self, id: int, /) -> int:
        return self[id][1]

    def get_child_id(self, pid: int, name: str, /) -> int:
        return self._pid_to_children[pid][name]

    def get_id(self, patht: Sequence[str], /, parent_id: int = 0) -> int:
        if not patht:
            return parent_id
        if not patht[0]:
            parent_id = 0
        pid_to_children = self._pid_to_children
        for name in patht:
            if name:
                parent_id = pid_to_children[parent_id][Str(name)]
        return parent_id

    def clear(self, top_id: int = 0, /):
        if top_id:
            self.data.pop(top_id, None)
            if children := self._pid_to_children.pop(top_id, None):
                for cid in children.values():
                    self.clear(cid)
        else:
            self.data.clear()
            self._pid_to_children.clear()

    def get_ancestor(self, id: int = 0, /) -> AncestorDict:
        if not id:
            return {"id": 0, "parent_id": 0, "name": ""}
        name, pid = self[id]
        return {"id": id, "parent_id": pid, "name": str(name)}

    def get_ancestors(self, id: int = 0, /) -> list[AncestorDict]:
        if not id:
            return [self.get_ancestor()]
        ancestors: list[AncestorDict] = []
        add_ancestor = ancestors.append
        while id:
            name, pid = self[id]
            add_ancestor({"id": id, "parent_id": pid, "name": str(name)})
            id = pid
        add_ancestor(self.get_ancestor())
        ancestors.reverse()
        return ancestors

    def get_patht(self, id: int = 0, /) -> list[str]:
        if not id:
            return [""]
        patht: list[str] = []
        add_part = patht.append
        while id:
            name, id = self[id]
            add_part(str(name))
        add_part("")
        patht.reverse()
        return patht

    def iter_children(self, pid: int = 0, /) -> Iterator[AncestorDict]:
        return ({"id": cid, "parent_id": pid, "name": str(name)} for name, cid in self._pid_to_children[pid].items())

    def iter_descendants(self, top_id: int = 0, /) -> Iterator[AncestorDict]:
        for attr in self.iter_children(top_id):
            yield attr
            yield from self.iter_descendants(attr["id"])

