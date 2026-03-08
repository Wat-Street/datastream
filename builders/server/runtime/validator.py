class ValidationError(Exception):
    pass


# Values must be valid arguments to isinstance (type or tuple of types)
TYPE_MAP = {
    "str": str,
    "int": int,
    "float": (int, float),  # accept int as valid float
    "bool": bool,
}


def validate(data: dict, schema: dict[str, str]) -> None:
    """Validate that data matches the declared schema.

    Checks that all declared keys are present and values match declared types.
    Raises ValidationError on failure.
    """
    for key, type_name in schema.items():
        if key not in data:
            raise ValidationError(f"Missing key '{key}' in builder output")

        if type_name not in TYPE_MAP:
            raise ValidationError(
                f"Unknown type '{type_name}' in schema for key '{key}'"
            )
        expected = TYPE_MAP[type_name]

        if not isinstance(data[key], expected):  # type: ignore[arg-type]  # expected is always a concrete type from TYPE_MAP
            raise ValidationError(
                f"Key '{key}' expected type '{type_name}', "
                f"got '{type(data[key]).__name__}'"
            )
