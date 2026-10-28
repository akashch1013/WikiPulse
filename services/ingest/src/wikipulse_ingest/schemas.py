import json
import re
from pathlib import Path

from jsonschema import Draft7Validator

UPSTREAM_ROOT = Path(__file__).parent / "schemas" / "upstream"
PAGE_CHANGE = "mediawiki/page/change"

_DECLARED = re.compile(r"^/(?P<title>[a-z0-9_/]+)/(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")

Version = tuple[int, int, int]


class SchemaMismatch(ValueError):
    pass


class UnknownSchemaMajor(ValueError):
    pass


class UpstreamSchemas:
    def __init__(self, title: str = PAGE_CHANGE, root: Path = UPSTREAM_ROOT) -> None:
        self.title = title
        self._validators: dict[Version, Draft7Validator] = {}
        for path in (root / title).glob("*.json"):
            major, minor, patch = (int(part) for part in path.stem.split("."))
            self._validators[(major, minor, patch)] = Draft7Validator(json.loads(path.read_text()))
        if not self._validators:
            raise FileNotFoundError(f"no vendored schemas under {root / title}")

    def resolve(self, declared: str) -> tuple[Draft7Validator, bool]:
        """Return the validator for a declared $schema and whether it is a newer, unvendored minor."""
        match = _DECLARED.match(declared)
        if match is None or match["title"] != self.title:
            raise SchemaMismatch(f"unexpected $schema {declared!r}")
        version = (int(match["major"]), int(match["minor"]), int(match["patch"]))
        if version in self._validators:
            return self._validators[version], False
        # Wikimedia only allows backward-compatible changes within a major version, so the newest
        # vendored minor of the same major still validates every field WikiPulse reads.
        same_major = sorted(v for v in self._validators if v[0] == version[0])
        if not same_major:
            raise UnknownSchemaMajor(f"no vendored schema with major version {version[0]} for {declared!r}")
        return self._validators[same_major[-1]], True
