import math
import pathlib
from collections.abc import Iterable


class Dtype:
    def __init__(self, name, representational_string=None):
        self.name = name
        self.repr = representational_string or name

    def __repr__(self):
        return f"dtype('{self.repr}')"

    def __str__(self):
        return self.name


DTYPES = {
    object: Dtype("object", "O"),
    float: Dtype("float64"),
    int: Dtype("int64"),
    bool: Dtype("bool"),
}

PARSERS = {
    object: lambda s: s,
    float: lambda s: (math.nan if s == "None" else float(s)),
    int: int,
    bool: lambda s: (s == "True"),
}

TYPE_CONVERTERS = {
    DTYPES[object]: lambda x: x,
    DTYPES[float]: lambda s: (math.nan if s == None else float(s)),
    DTYPES[int]: int,
    DTYPES[bool]: bool,
}


def least_common_superclass(types):
    if len(types) == 1:
        return next(iter(types))
    return float if set(types) == {float, int} else object


def types(elements: Iterable):
    return {
        next(t for t in (bool, int, float, object) if isinstance(x, t))
        for x in (math.nan if x is None else x for x in elements)
    }


def least_common_type(elements: Iterable):
    return least_common_superclass(types(elements))


def guess_type(s: str):
    if s in ("True", "False"):
        return bool
    typ = object
    try:
        for t in (float, int):
            t(s)
            typ = t
    except:
        pass
    return typ


def stringify_elements(elements: Iterable, typ: Dtype = DTYPES[object]):
    if typ == DTYPES[float]:
        width = min(6, max(len(str(x).split(".")[1]) for x in elements))
        elems = [f"{x:.{width}f}" for x in elements]
    else:
        elems = tuple(map(str, elements))
    width = max(map(len, elems))
    return [x.rjust(width) for x in elems]


class Series:
    def __init__(
        self,
        data=None,
        index=None,
        dtype=None,
        name=None,
        copy=None,
        fastpath=None,
    ):
        if any(field != None for field in (index, copy, fastpath)):
            raise NotImplementedError()
        data = tuple(data) if not isinstance(data, str) else (data,)
        self.name = name
        self.dtype = DTYPES.get(dtype or least_common_type(data), dtype)
        self.data = [TYPE_CONVERTERS[self.dtype](x) for x in data]

    def __repr__(self):
        indexes = stringify_elements(range(len(self.data)))
        values = stringify_elements(self.data, self.dtype)
        return (
            "".join("    ".join(row) + "\n" for row in zip(indexes, values))
            + (f"Name: {self.name}, " if self.name != None else "")
            + f"dtype: {self.dtype}"
        )

    def max(self):
        return max(self.data, default=math.nan)


class DataFrame:
    def __init__(
        self, data=None, index=None, columns=None, dtype=None, copy=None
    ):
        if any(field != None for field in (index, columns, dtype, copy)):
            raise NotImplementedError()
        match data:
            case dict(d):
                columns, data = map(tuple, (d.keys(), d.values()))
            case _:
                raise NotImplementedError()
        self.columns = columns
        self.data = data
        self.dtypes = [DTYPES[least_common_type(column)] for column in data]
        self.shape = (len(columns), len(data[0]))

    def __repr__(self):
        content = [[str(cell) for cell in col] for col in self.data]
        # HACK#1: bug of small padding between string columns requires
        #         small paddings between inside header if dtype is obj
        widths = [
            max(max(map(len, content)), len(head) - (typ == DTYPES[object])) + 2
            for head, content, typ in zip(self.columns, content, self.dtypes)
        ]
        header = (head.rjust(w) for w, head in zip(widths, self.columns))
        index_width = len(str(self.shape[1]))
        result = [" " * index_width + "".join(header)]
        for i, row in enumerate(zip(*content)):
            row = (cell.rjust(w) for w, cell in zip(widths, row))
            result.append(str(i).rjust(index_width) + "".join(row))
        return "\n".join(result)

    def __getitem__(self, key):
        try:
            i = self.columns.index(key)
            return Series(self.data[i], dtype=self.dtypes[i], name=key)
        except ValueError:
            raise KeyError(key)


def read_csv(filepath: str | pathlib.Path, **kwargs):
    if kwargs:
        raise NotImplementedError()
    rows = pathlib.Path(filepath).read_text().strip().split("\n")
    rows = [[x or "NaN" for x in r.split(",")] for r in rows]
    header, data = rows[0], rows[1:]
    data = [[r[i] for r in data] for i in range(len(header))]
    dtypes = [least_common_superclass(set(map(guess_type, r))) for r in data]
    d = {
        header: list(map(PARSERS[typ], column))
        for header, typ, column in zip(header, dtypes, data)
    }
    return DataFrame(d)
