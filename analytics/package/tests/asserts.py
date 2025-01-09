import re


def strip_whitespace(string: str) -> str:
    return re.sub(r"\s+", " ", string).strip()


def assert_equal_ignoring_whitespace(actual, expected):
    assert strip_whitespace(actual) == strip_whitespace(expected)
