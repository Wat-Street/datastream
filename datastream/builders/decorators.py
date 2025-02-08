from datetime import date


class data_source:
    """
    Create a builder for a dataset without dependencies.
    """

    def __init__(
        self, *, name: str, version: str, calendar, start_date: date, fields
    ):
        self.name = name
        self.version = version
        self.calendar = calendar
        self.start_date = start_date
        self.fields = fields

    def __call__(self, builder):
        def wrapper():
            return builder()

        return wrapper


class dataset:
    """
    Create a builder for a dataset with dependencies.
    """

    def __init__(
        self,
        *,
        name: str,
        version: str,
        calendar,
        start_date: date,
        dependencies,
        fields
    ):
        self.name = name
        self.version = version
        self.calendar = calendar
        self.start_date = start_date
        self.dependencies = dependencies
        self.fields = fields

    def __call__(self, builder):
        def wrapper():
            return builder()

        return wrapper
