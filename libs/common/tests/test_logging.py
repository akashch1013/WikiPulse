import json
import logging

import pytest

from wikipulse_common.logging import configure_logging


@pytest.fixture
def restore_root_logger():
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


def test_log_lines_are_json_with_extra_fields(capsys, restore_root_logger):
    configure_logging("ingest", "INFO")
    logging.getLogger("wikipulse_ingest.checkpoint").info("checkpoint written", extra={"partition": 3})
    logging.getLogger("wikipulse_ingest").debug("not emitted at INFO")

    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 1
    line = json.loads(lines[0])
    assert line["service"] == "ingest"
    assert line["level"] == "INFO"
    assert line["event"] == "checkpoint written"
    assert line["logger"] == "wikipulse_ingest.checkpoint"
    assert line["partition"] == 3
    assert line["ts"].endswith("+00:00")


def test_exceptions_are_included(capsys, restore_root_logger):
    configure_logging("api", "INFO")
    try:
        raise ValueError("bad cursor")
    except ValueError:
        logging.getLogger("wikipulse_api").exception("request failed")
    line = json.loads(capsys.readouterr().out)
    assert "ValueError: bad cursor" in line["exception"]
