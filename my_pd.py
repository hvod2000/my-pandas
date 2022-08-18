import math
import pathlib
from collections.abc import Iterable

SUPPORTED_TYPES = {
    object: "object",
    float: "float64",
    int: "int64",
    bool: "bool",
}


def least_common_supertype(elements: Iterable):
    lcs_index, supported_types = -1, tuple(reversed(SUPPORTED_TYPES.keys()))
    for x in (x for x in elements if x != None):
        for typ_index, typ in enumerate(supported_types):
            if isinstance(x, typ):
                break
        lcs_index = max(lcs_index, typ_index)
    return supported_types[lcs_index]


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
        self.dtype = dtype if dtype != None else least_common_supertype(data)

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
        self.dtype = [least_common_supertype(column) for column in data]
        self.shape = (len(columns), len(data[0]))

    def __repr__(self):
        content = [[str(cell) for cell in col] for col in self.data]
        widths = [
            max(max(map(len, content)), len(head))
            for head, content in zip(self.columns, content)
        ]
        header = (head.rjust(w) for w, head in zip(widths, self.columns))
        index_width = len(str(self.shape[1]))
        result = [" " * index_width + "  " + "  ".join(header)]
        for i, row in enumerate(zip(*content)):
            row = (cell.rjust(w) for w, cell in zip(widths, row))
            result.append(str(i).rjust(index_width) + "  " + "  ".join(row))
        return "\n".join(result)

    def __getitem__(self, key):
        try:
            i = self.columns.index(key)
            return Series(self.data[i], dtype=self.dtype[i], name=key)
        except ValueError:
            raise KeyError(key)


def read_csv( filepath: str | pathlib.Path, **kwargs):
    if kwargs:
        raise NotImplementedError()
    rows = pathlib.Path(filepath).read_text().strip().split("\n")
    rows = [r.split(",") for r in rows]
    header, data = rows[0], rows[1:]
    d = {h:[row[i] for row in data] for i, h in enumerate(header)}
    return DataFrame(d)
