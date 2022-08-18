import math
import pathlib
from collections.abc import Iterable

SUPPORTED_TYPES = {
    object: "object",
    float: "float64",
    int: "int64",
    bool: "bool",
}

PARSERS = {
    object: lambda s: s,
    float: lambda s: (math.nan if s == "None" else float(s)),
    int: int,
    bool: lambda s: (s == "True"),
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


def stringify_elements(elements: Iterable):
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
        self.name = name
        self.data = tuple(data) if not isinstance(data, str) else (data,)
        self.dtype = dtype if dtype != None else least_common_type(data)

    def __repr__(self):
        columns = map(stringify_elements, (range(len(self.data)), self.data))
        return (
            "".join("    ".join(row) + "\n" for row in zip(*columns))
            + (f"Name: {self.name}, " if self.name != None else "")
            + f"dtype: {SUPPORTED_TYPES[self.dtype]}"
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
        self.dtype = [least_common_type(column) for column in data]
        self.shape = (len(columns), len(data[0]))

    def __repr__(self):
        content = [[str(cell) for cell in col] for col in self.data]
        # HACK#1: bug of small padding between string columns requires
        #         small paddings between inside header if dtype is obj
        widths = [
            max(max(map(len, content)), len(head) - (typ == object)) + 2
            for head, content, typ in zip(self.columns, content, self.dtype)
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
            return Series(self.data[i], dtype=self.dtype[i], name=key)
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
