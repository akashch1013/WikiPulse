from pathlib import Path

from wikipulse_common.config import load_settings
from wikipulse_common.kafka import consumer_config, producer_config

REPO_CONFIG = Path(__file__).resolve().parents[3] / "config" / "wikipulse.toml"


def test_producer_is_idempotent_with_full_acks():
    config = producer_config(load_settings(REPO_CONFIG).kafka, client_id="ingest-1")
    assert config["enable.idempotence"] is True
    assert config["acks"] == "all"
    assert config["compression.type"] == "zstd"
    assert config["bootstrap.servers"] == "localhost:9092"


def test_consumer_commits_manually_from_earliest():
    config = consumer_config(load_settings(REPO_CONFIG).kafka, group_id="stream-processor", client_id="sp-1")
    assert config["group.id"] == "stream-processor"
    assert config["enable.auto.commit"] is False
    assert config["auto.offset.reset"] == "earliest"
