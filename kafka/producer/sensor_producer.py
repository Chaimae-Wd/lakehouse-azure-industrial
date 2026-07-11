"""
Producteur Kafka simulant des capteurs industriels.

Le script génère continuellement des mesures de température,
pression, vibration et énergie.
"""

import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "industrial-sensor-readings",
)

EVENT_INTERVAL_SECONDS = float(
    os.getenv("EVENT_INTERVAL_SECONDS", "1")
)

RUNNING = True


SENSOR_CONFIGURATIONS: Dict[str, Dict[str, float | str]] = {
    "TEMPERATURE": {
        "unit": "C",
        "minimum": 20.0,
        "maximum": 110.0,
    },
    "VIBRATION": {
        "unit": "mm/s",
        "minimum": 0.0,
        "maximum": 18.0,
    },
    "PRESSURE": {
        "unit": "bar",
        "minimum": 1.0,
        "maximum": 22.0,
    },
    "ENERGY": {
        "unit": "kWh",
        "minimum": 10.0,
        "maximum": 650.0,
    },
}


def configure_logging() -> logging.Logger:
    """Configurer les journaux du producteur."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    return logging.getLogger("industrial_sensor_producer")


def stop_producer(signum, frame) -> None:
    """Arrêter proprement le générateur."""

    del signum, frame

    global RUNNING
    RUNNING = False


def generate_sensor_event() -> dict:
    """Générer une mesure industrielle réaliste."""

    sensor_type = random.choice(
        list(SENSOR_CONFIGURATIONS.keys())
    )

    configuration = SENSOR_CONFIGURATIONS[sensor_type]

    machine_id = random.randint(1, 50)
    sensor_id = random.randint(1, 150)

    value = round(
        random.uniform(
            float(configuration["minimum"]),
            float(configuration["maximum"]),
        ),
        3,
    )

    return {
        "event_id": (
            f"{machine_id}-{sensor_id}-"
            f"{time.time_ns()}"
        ),
        "sensor_id": sensor_id,
        "machine_id": machine_id,
        "sensor_type": sensor_type,
        "value": value,
        "unit": configuration["unit"],
        "event_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
    }


def delivery_report(error, message) -> None:
    """Afficher le résultat de l'envoi Kafka."""

    if error is not None:
        logging.error(
            "Échec de livraison Kafka : %s",
            error,
        )
        return

    logging.info(
        "Événement envoyé | topic=%s partition=%s offset=%s",
        message.topic(),
        message.partition(),
        message.offset(),
    )


def main() -> None:
    """Produire continuellement des événements Kafka."""

    logger = configure_logging()

    signal.signal(signal.SIGINT, stop_producer)
    signal.signal(signal.SIGTERM, stop_producer)

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "industrial-sensor-producer",
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    logger.info(
        "Démarrage du producteur vers %s / %s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
    )

    produced_count = 0

    try:
        while RUNNING:
            event = generate_sensor_event()

            producer.produce(
                topic=KAFKA_TOPIC,
                key=str(event["machine_id"]),
                value=json.dumps(event),
                callback=delivery_report,
            )

            producer.poll(0)

            produced_count += 1

            logger.info(
                "Mesure générée : %s",
                event,
            )

            time.sleep(EVENT_INTERVAL_SECONDS)

    except Exception:
        logger.exception(
            "Erreur pendant la production des événements."
        )
        sys.exit(1)

    finally:
        logger.info(
            "Arrêt du producteur après %s événements.",
            produced_count,
        )

        producer.flush(10)


if __name__ == "__main__":
    main()
    