import pytest
from runtime.validator import ValidationError, validate


def test_valid_data_passes() -> None:
    """Valid data matching schema raises nothing."""
    validate({"ticker": "AAPL", "price": 100}, {"ticker": "str", "price": "int"})


def test_empty_schema_passes_anything() -> None:
    """Empty schema means no constraints."""
    validate({"anything": 123, "goes": "here"}, {})


def test_extra_keys_allowed() -> None:
    """Data with extra keys beyond schema passes."""
    validate({"ticker": "AAPL", "extra": 999}, {"ticker": "str"})


def test_missing_key_raises() -> None:
    """Missing required key raises ValidationError."""
    with pytest.raises(ValidationError, match="Missing key 'ticker'"):
        validate({}, {"ticker": "str"})


def test_unknown_type_in_schema_raises() -> None:
    """Unknown type string in schema raises ValidationError."""
    with pytest.raises(ValidationError, match="Unknown type 'datetime'"):
        validate({"x": 1}, {"x": "datetime"})


def test_type_mismatch_str_raises() -> None:
    """Int where str expected raises ValidationError."""
    with pytest.raises(ValidationError, match="expected type 'str'"):
        validate({"name": 123}, {"name": "str"})


def test_type_mismatch_int_raises() -> None:
    """Str where int expected raises ValidationError."""
    with pytest.raises(ValidationError, match="expected type 'int'"):
        validate({"count": "five"}, {"count": "int"})


def test_float_accepts_int() -> None:
    """Int passes float schema since int is a valid float."""
    validate({"value": 42}, {"value": "float"})


def test_float_accepts_actual_float() -> None:
    """Float passes float schema."""
    validate({"value": 3.14}, {"value": "float"})


def test_float_rejects_str() -> None:
    """Str where float expected raises ValidationError."""
    with pytest.raises(ValidationError, match="expected type 'float'"):
        validate({"value": "pi"}, {"value": "float"})


def test_bool_type() -> None:
    """True passes bool schema."""
    validate({"flag": True}, {"flag": "bool"})


def test_bool_rejects_int() -> None:
    """1 fails bool schema since int is not bool."""
    with pytest.raises(ValidationError, match="expected type 'bool'"):
        validate({"flag": 1}, {"flag": "bool"})
