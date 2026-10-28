from wikipulse_common.config import KafkaSettings


def producer_config(settings: KafkaSettings, client_id: str) -> dict[str, str | int | bool]:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": client_id,
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": settings.compression,
        "linger.ms": 50,
    }


def consumer_config(settings: KafkaSettings, group_id: str, client_id: str) -> dict[str, str | int | bool]:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": client_id,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
