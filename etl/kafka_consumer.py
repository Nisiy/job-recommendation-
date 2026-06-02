import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC     = "jobs_raw"
RAW_PATH        = "/opt/airflow/datasource/raw/jobs_raw.json"


def consume_jobs(bootstrap=KAFKA_BOOTSTRAP, timeout_ms=15000):
    # Fresh group_id every run → always reads from earliest, never skips messages
    group_id = f"airflow-etl-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    print(f"Consumer group: {group_id}")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[bootstrap],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=group_id,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=timeout_ms,
        api_version=(2, 5, 0),
    )

    jobs = []
    for msg in consumer:
        jobs.append(msg.value)
    consumer.close()

    if not jobs:
        raise ValueError(
            f"No messages consumed from topic '{KAFKA_TOPIC}'. "
            "Run the produce task first."
        )

    os.makedirs(os.path.dirname(RAW_PATH), exist_ok=True)
    with open(RAW_PATH, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)

    print(f"✅ Consumed {len(jobs)} jobs → {RAW_PATH}")
    return RAW_PATH


if __name__ == "__main__":
    consume_jobs()