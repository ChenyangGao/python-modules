cimport cython
from cpython.dict cimport PyDict_GetItemWithError
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from cpython.unicode cimport (
    PyUnicode_GET_LENGTH,
    PyUnicode_KIND,
    PyUnicode_DATA,
    PyUnicode_READ,
    PyUnicode_FromKindAndData,
    PyUnicode_4BYTE_KIND, 
)


from collections.abc import Callable, Mapping, Sequence

def make_get(m, /) -> Callable:
    if isinstance(m, Mapping):
        call = m.__getitem__
    elif callable(m):
        call = m.__call__
    elif type(m) is tuple and len(m) == 2 and isinstance(m[0], Sequence):
        l, r = m
        def call(k, /, *, _indexof=l.index, _getval=r.__getiem__):
            try:
                return _getval[_indexof(k)]
            except (ValueError, LookupError):
                return k
        return call
    else:
        if not isinstance(m, Sequence):
            m = tuple(m)
        def call(k, /, *, _pairs=m):
            for a, b in _pairs:
                if a is k or a == k:
                    return b
            return k
        return call
    def get(k, /, *, _call=call):
        try:
            return _call(k)
        except LookupError:
            return k
    return get




def make_get(m, /) -> Callable:
    call = m.__getitem__
    def get(k, /, *, _call=call):
        try:
            return _call(k)
        except LookupError:
            return k
    return get

def make_get(m, /):
    def get(k, /, m=m.__getitem__):
        try:
            return m(k)
        except LookupError:
            return k
    return get


def make_get(m, /):
    def get(k, /, m=m):
        try:
            return m[k]
        except LookupError:
            return k
    return get



@cython.boundscheck(False)
@cython.wraparound(False)
cpdef unicode str_translate(unicode s, unicode src, unicode dst):
    """Batch replace a sequence of character mappings in a string.

    :param s: The original string.
    :param src: The sequence of characters before mapping. They will be compared in order; when a match is found, it will be replaced by the character at the corresponding position in ``dst``.
    :param dst: The sequence of characters after mapping. Its length must not be shorter than ``src``.

    :return: The replaced string.
    """
    cdef:
        Py_ssize_t s_len = PyUnicode_GET_LENGTH(s)
        Py_ssize_t map_len = PyUnicode_GET_LENGTH(src)

    if s_len == 0 or map_len == 0:
        return s
    elif map_len > PyUnicode_GET_LENGTH(dst):
        raise ValueError("src and dst must be of equal length")

    cdef:
        Py_ssize_t i, j
        int s_kind = PyUnicode_KIND(s)
        void* s_data = PyUnicode_DATA(s)
        int src_kind = PyUnicode_KIND(src)
        void* src_data = PyUnicode_DATA(src)
        int dst_kind = PyUnicode_KIND(dst)
        void* dst_data = PyUnicode_DATA(dst)
        Py_UCS4 cur_char
        Py_UCS4* buf

    buf = <Py_UCS4*>PyMem_Malloc(s_len * sizeof(Py_UCS4))
    if not buf:
        raise MemoryError("buffer allocation failed")
    try:
        for i in range(s_len):
            cur_char = PyUnicode_READ(s_kind, s_data, i)
            for j in range(map_len):
                if cur_char == PyUnicode_READ(src_kind, src_data, j):
                    buf[i] = PyUnicode_READ(dst_kind, dst_data, j)
                    break
            else:
                buf[i] = cur_char
        return PyUnicode_FromKindAndData(PyUnicode_4BYTE_KIND, buf, s_len)
    finally:
        PyMem_Free(buf)


@cython.boundscheck(False)
@cython.wraparound(False)
cpdef unicode str_translate_by_dict(unicode s, dict maps):
    """Batch replace a sequence of character mappings in a string.

    :param s: The original string.
    :param maps: The sequence of characters before mapping. They will be compared in order; when a match is found, it will be replaced by the character at the corresponding position in ``dst``.

    :return: The replaced string.
    """
    cdef:
        Py_ssize_t s_len = PyUnicode_GET_LENGTH(s)
        int s_kind = PyUnicode_KIND(s)
        void* s_data = PyUnicode_DATA(s)
        Py_UCS4 cur_char
        Py_UCS4* buf

    if s_len == 0:
        return s
    buf = <Py_UCS4*>PyMem_Malloc(s_len * sizeof(Py_UCS4))
    if not buf:
        raise MemoryError("buffer allocation failed")
    try:
        for i in range(s_len):
            cur_char = PyUnicode_READ(s_kind, s_data, i)
            buf[i] = maps.get(cur_char, cur_char)
        return PyUnicode_FromKindAndData(PyUnicode_4BYTE_KIND, buf, s_len)
    finally:
        PyMem_Free(buf)




@cython.boundscheck(False)
@cython.wraparound(False)
cpdef unicode str_translate_by_map(unicode s, dict maps):
    """Batch replace a sequence of character mappings in a string.

    :param s: The original string.
    :param maps: The sequence of characters before mapping. They will be compared in order; when a match is found, it will be replaced by the character at the corresponding position in ``dst``.

    :return: The replaced string.
    """
    cdef:
        Py_ssize_t s_len = PyUnicode_GET_LENGTH(s)
        int s_kind = PyUnicode_KIND(s)
        void* s_data = PyUnicode_DATA(s)
        Py_UCS4* buf

    if s_len == 0:
        return s
    buf = <Py_UCS4*>PyMem_Malloc(s_len * sizeof(Py_UCS4))
    if not buf:
        raise MemoryError("buffer allocation failed")
    try:
        get = make_get(maps)
        for i in range(s_len):
            buf[i] = get(PyUnicode_READ(s_kind, s_data, i))
        return PyUnicode_FromKindAndData(PyUnicode_4BYTE_KIND, buf, s_len)
    finally:
        PyMem_Free(buf)

