import logging
import signal
import sys
import threading

import httpx
import psycopg
from confluent_kafka import Producer

from wikipulse_common.config import load_settings
from wikipulse_common.kafka import producer_config
from wikipulse_common.logging import configure_logging
from wikipulse_common.telemetry import configure_metrics, get_meter
from wikipulse_ingest.checkpoint import CheckpointStore
from wikipulse_ingest.process import EventProcessor
from wikipulse_ingest.schemas import UpstreamSchemas
from wikipulse_ingest.service import DeliveryFailed, Ingestor

SERVICE = "ingest"
CONSUMER = "ingest:mediawiki.page_change.v1"
# EventStreams sends several events per second across all wikis; a minute of silence means a dead socket.
STREAM_TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)


def main() -> int:
    settings = load_settings()
    configure_logging(SERVICE, settings.telemetry.log_level)
    configure_metrics(settings.telemetry, SERVICE)
    log = logging.getLogger(SERVICE)

    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())

    pg = settings.postgres
    with (
        psycopg.connect(
            host=pg.host,
            port=pg.port,
            dbname=pg.database,
            user=pg.user,
            password=pg.password.get_secret_value(),
            autocommit=True,
            application_name=SERVICE,
        ) as connection,
        httpx.Client(timeout=STREAM_TIMEOUT, follow_redirects=True) as client,
    ):
        producer = Producer(producer_config(settings.kafka, client_id=SERVICE))
        ingestor = Ingestor(
            settings,
            store=CheckpointStore(connection, CONSUMER),
            producer=producer,
            processor=EventProcessor(
                UpstreamSchemas(), settings.privacy.require_hmac_key(), settings.stream.max_payload_bytes
            ),
            client=client,
            meter=get_meter(SERVICE),
        )
        log.info("starting", extra={"stream": settings.stream.url, "topic": settings.stream.topic})
        try:
            ingestor.run(stop)
        except DeliveryFailed as exc:
            log.error("stopping: kafka delivery failed; checkpoint not advanced", extra={"error": str(exc)})
            return 1
    log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
