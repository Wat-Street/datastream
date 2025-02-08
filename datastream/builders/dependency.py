from dataclasses import dataclass, field


@dataclass
class Dependency:
    name: str
    version: str

    lookback: int = field(default=1, kw_only=True)
    lookforward: int = field(default=0, kw_only=True)
