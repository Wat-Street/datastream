from dataclasses import dataclass, field
from enum import StrEnum


class FieldType(StrEnum):
    _float = "float"
    _int = "int"
    _str = "str"
    # more as needed

    # add functionality to convert these to sqlalchemy schema types


@dataclass
class Field:
    name: str
    _type: FieldType = field(kw_only=True)
