#!/usr/bin/env python
# utility methods for for addressing snake_cased data via moztelemetry
# https://github.com/acmiyaguchi/test-casing/
import regex as re
import collections

# Search for all camelCase situations in reverse with arbitrary lookaheads.
REV_WORD_BOUND_PAT = re.compile(
    r"""(?V1)
    \b                                  # standard word boundary
    |(?<=[a-z][A-Z])(?=\d*[A-Z])        # A7Aa -> A7|Aa boundary
    |(?<=[a-z][A-Z])(?=\d*[a-z])        # a7Aa -> a7|Aa boundary
    |(?<=[A-Z])(?=\d*[a-z])             # a7A -> a7|A boundary
    """,
    re.VERBOSE,
)


def snake_case(line: str) -> str:
    # replace non-alphanumeric characters with spaces in the reversed line
    subbed = re.sub(r"[^\w]|_", " ", line[::-1])

    # apply the regex on the reversed string
    words = REV_WORD_BOUND_PAT.split(subbed)

    # filter spaces between words and snake_case and reverse again
    return "_".join([w.lower() for w in words if w.strip()])[::-1]


def split_snake_case(line):
    return "/".join(list(map(snake_case, line.split("/"))))


class SnakeCaseDictOld(dict):
    class Key(str):
        def __init__(self, key):
            str.__init__(key)

        def __hash__(self):
            return hash(snake_case(self))

        def __eq__(self, other):
            return snake_case(self) == snake_case(other)

    def __init__(self, data=None):
        super(SnakeCaseDict, self).__init__()
        if data is None:
            data = {}
        for key, val in data.items():
            self[key] = val

    def __contains__(self, key):
        key = self.Key(key)
        return super(SnakeCaseDict, self).__contains__(key)

    def __setitem__(self, key, value):
        key = self.Key(key)
        super(SnakeCaseDict, self).__setitem__(key, value)

    def __getitem__(self, key):
        key = self.Key(key)
        return super(SnakeCaseDict, self).__getitem__(key)


class SnakeCaseDict(collections.MutableMapping):
    def __init__(self, *args, **kwargs):
        self.data = dict(*args, **kwargs)

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __contains__(self, key):
        a = key in self.data
        if a:
            return a
        return snake_case(key) in self.data

    def __getitem__(self, key):
        try:
            return self.data[key]
        except:
            return self.data[snake_case(key)]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __delitem__(self, key):
        self.data.delete(key)
