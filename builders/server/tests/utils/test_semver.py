import pytest
from utils.semver import SemVer


def test_parse_basic():
    sv = SemVer.parse("1.2.3")
    assert sv.major == 1
    assert sv.minor == 2
    assert sv.patch == 3


def test_parse_zeros():
    assert SemVer.parse("0.0.0") == SemVer(0, 0, 0)


def test_parse_large_numbers():
    assert SemVer.parse("100.200.300") == SemVer(100, 200, 300)


def test_parse_strips_whitespace():
    assert SemVer.parse("  1.2.3  ") == SemVer(1, 2, 3)


@pytest.mark.parametrize(
    "bad_input",
    [
        "",
        "1",
        "1.2",
        "1.2.3.4",
        "v1.2.3",
        "1.2.x",
        "a.b.c",
        "1.2.-3",
        "-1.2.3",
        "01.2.3",
        "1.02.3",
        "1.2.03",
    ],
)
def test_parse_invalid_strings(bad_input: str):
    with pytest.raises(ValueError, match="invalid semver string"):
        SemVer.parse(bad_input)


def test_str_roundtrip():
    assert str(SemVer.parse("1.2.3")) == "1.2.3"


def test_str_from_constructor():
    assert str(SemVer(0, 1, 0)) == "0.1.0"


def test_equal():
    assert SemVer.parse("1.2.3") == SemVer.parse("1.2.3")


def test_not_equal():
    assert SemVer.parse("1.2.3") != SemVer.parse("1.2.4")


def test_major_ordering():
    assert SemVer(1, 0, 0) < SemVer(2, 0, 0)


def test_minor_ordering():
    assert SemVer(1, 0, 0) < SemVer(1, 1, 0)


def test_patch_ordering():
    assert SemVer(1, 0, 0) < SemVer(1, 0, 1)


def test_major_takes_precedence():
    assert SemVer(2, 0, 0) > SemVer(1, 9, 9)


def test_frozen():
    sv = SemVer(1, 2, 3)
    with pytest.raises(AttributeError):
        sv.major = 5  # type: ignore


def test_hashable():
    a = SemVer.parse("1.2.3")
    b = SemVer.parse("1.2.3")
    assert hash(a) == hash(b)
    assert len({a, b}) == 1
