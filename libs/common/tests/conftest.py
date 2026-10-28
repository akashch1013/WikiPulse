import os

import pytest


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    # `make test` exports .env; the tests assert against the committed configuration only.
    for name in list(os.environ):
        if name.startswith("WIKIPULSE_"):
            monkeypatch.delenv(name)
