import re
from dataclasses import dataclass

# no leading zeros allowed per semver spec
_SEMVER_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")


@dataclass(frozen=True, order=True)
class SemVer:
    """Semantic version with major.minor.patch components."""

    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, version_str: str) -> "SemVer":
        """Parse a version string like '1.2.3' into a SemVer instance."""
        match = _SEMVER_RE.match(version_str.strip())
        if not match:
            raise ValueError(
                f"invalid semver string: '{version_str}' "
                "(expected format: major.minor.patch)"
            )
        return cls(
            major=int(match.group(1)),
            minor=int(match.group(2)),
            patch=int(match.group(3)),
        )

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"
