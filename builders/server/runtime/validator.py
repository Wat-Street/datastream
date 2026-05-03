from runtime.config import SchemaType


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


def validate_rows(data_list: list[dict], schema: dict[str, SchemaType]) -> None:
    """Validate each dict in a list against the declared schema."""
    if type(data_list) is not list:
        raise ValidationError(f"Data received is of type: '{type(data_list).__name__}', however a list is expected")
    else:
        for data in data_list:
            validate(data, schema)
