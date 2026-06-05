from core.runtime.config import SchemaType


class ValidationError(Exception):
    pass


def validate(data: dict, schema: dict[str, SchemaType]) -> None:
    """Validate that data matches the declared schema.

    Checks that all declared keys are present and values match declared types.
    Raises ValidationError on failure.
    """
    for key, schema_type in schema.items():
        if key not in data:
            raise ValidationError(f"Missing key '{key}' in builder output")

        expected = schema_type.to_type()
        if not isinstance(data[key], expected):
            raise ValidationError(
                f"Key '{key}' expected type '{schema_type.value}', "
                f"got '{type(data[key]).__name__}'"
            )


def validate_rows(data_list: object, schema: dict[str, SchemaType]) -> None:
    """Validate each dict in a list against the declared schema."""
    if not isinstance(data_list, list):
        raise ValidationError(
            f"Builder output expected a list of rows, got '{type(data_list).__name__}'"
        )

    for index, data in enumerate(data_list):
        if not isinstance(data, dict):
            raise ValidationError(
                f"Builder output row {index} expected a dict, "
                f"got '{type(data).__name__}'"
            )
        validate(data, schema)
