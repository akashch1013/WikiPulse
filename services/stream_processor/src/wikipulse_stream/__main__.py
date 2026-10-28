import os
from pathlib import Path

from wikipulse_common.config import load_settings
from wikipulse_common.logging import configure_logging
from wikipulse_common.telemetry import configure_metrics
from wikipulse_stream.app import SERVICE, Topics, build_application

HEARTBEAT_PATH = Path("/tmp/wikipulse-stream.heartbeat")


def main() -> None:
    settings = load_settings()
    configure_logging(SERVICE, settings.telemetry.log_level)
    configure_metrics(settings.telemetry, SERVICE)
    topics = Topics(settings.stream.topic, settings.stream.dlq_topic, settings.stream.late_topic)
    state_dir = Path(os.environ.get("WIKIPULSE_STREAM_STATE_DIR", "state"))
    app, side_output = build_application(settings, topics, state_dir, HEARTBEAT_PATH)
    try:
        app.run()
    finally:
        side_output.flush()


if __name__ == "__main__":
    main()
