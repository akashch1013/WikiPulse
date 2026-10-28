import json
from pathlib import Path

import pytest

FIXTURE = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "page_change" / "events.jsonl"


@pytest.fixture(scope="session")
def fixture_lines() -> list[dict]:
    return [json.loads(line) for line in FIXTURE.read_text().splitlines()]


@pytest.fixture(scope="session")
def hmac_key() -> bytes:
    return bytes.fromhex("5f" * 32)
