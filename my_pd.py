import math
import pathlib
from itertools import starmap
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


def right_aligned(elements: list[str]):
    elements = list(elements)
    width = max(map(len, elements))
    return [x.rjust(width) for x in elements]


def left_aligned(elements: list[str]):
    elements = list(elements)
    width = max(map(len, elements))
    return [x.ljust(width) for x in elements]


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


def stringify_column(elements: Iterable, typ: Dtype = DTYPES[object]):
    if typ == DTYPES[float]:
        width = min(6, max(len(str(x).split(".")[1]) for x in elements))
        elems = [f"{x:.{width}f}" for x in elements]
        if any(len(x) >= 12 for x in elems):
            elems = [f"{x:e}" for x in elements]
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
        if isinstance(data, dict):
            indexes, data = tuple(data.keys()), tuple(data.values())
        elif not isinstance(data, Iterable) or isinstance(data, str):
            data, indexes = (data,), (0,)
        else:
            data = tuple(data)
            indexes = tuple(range(len(data)))
        data = tuple(data) if not isinstance(data, str) else (data,)
        self.name = name
        self.dtype = DTYPES.get(dtype or least_common_type(data), dtype)
        self.data = [TYPE_CONVERTERS[self.dtype](x) for x in data]
        self.index = indexes

    def abs(self):
        return Series([abs(x) for x in self.data], dtype=self.dtype)

    def max(self):
        return max(self.data, default=math.nan)

    def to_string(
        self,
        buf=None,
        na_rep="NaN",
        float_format=None,
        header=True,
        index=True,
        length=False,
        dtype=False,
        name=False,
        max_rows=None,
        min_rows=None,
    ):
        # HACK#4: It looks like the "header" parameter is just ignored...
        #         I can't find any information about what it does
        #         and why it's needed
        if self.dtype == DTYPES[float] and float_format is not None:
            lines = [
                na_rep if math.isnan(x) else float_format(x) for x in self.data
            ]
        elif self.dtype in (DTYPES[int], DTYPES[float]):
            width = min(6, max(len(f"{x}.".split(".")[1]) for x in self.data))
            lines = [
                na_rep if math.isnan(x) else f"{x: .{width}f}"
                for x in self.data
            ]
            if any(len(x) >= 12 for x in lines):
                lines = [f"{x: e}" for x in self.data]
        else:
            lines = [
                " " + (x if len(x) < 50 else x[:46] + "...")
                for x in map(str, self.data)
            ]
        lines = right_aligned(lines)
        if index:
            indexes = left_aligned(map(str, self.index))
            lines = [f"{i}   {x}" for i, x in zip(indexes, lines)]
        elif all(line.startswith(" ") for line in lines):
            lines = [line[1:] for line in lines]
        if max_rows != None and len(lines) > max_rows:
            if min_rows is None:
                min_rows = max_rows
            # HACK#2: in order to ignore lowest bit of min_rows
            #         we sometimes exceed max_rows
            # HACK#3: lower bit of min_rows is ignored
            lines[min_rows // 2 : -(min_rows // 2)] = [
                "..".rjust(len(lines[0]))
            ]
        last_line = (
            "\n"
            + (f"Name: {self.name}, " if name and self.name != None else "")
            + (f"Length: {len(self.data)}, " if length else "")
            + (f"dtype: {self.dtype}, " if dtype else "")
        )[:-2]
        return (buf.write if buf else lambda x: x)(
            "\n".join(lines) + last_line
        )

    def __repr__(self):
        if len(self.data) <= 60:
            return self.to_string(name=True, dtype=True)
        return self.to_string(name=True, dtype=True, length=True, max_rows=10)


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
        self.dtypes = [DTYPES[least_common_type(column)] for column in data]
        self.data = [
            [TYPE_CONVERTERS[t](x) for x in column]
            for column, t in zip(data, self.dtypes)
        ]
        self.shape = (len(columns), len(data[0]))

    def __repr__(self):
        content = list(starmap(stringify_column, zip(self.data, self.dtypes)))
        # HACK#1: bug of small padding between string columns requires
        #         small paddings between inside header if dtype is obj
        widths = [
            max(max(map(len, lines)), len(head) - (typ == DTYPES[object])) + 2
            for head, lines, typ in zip(self.columns, content, self.dtypes)
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
