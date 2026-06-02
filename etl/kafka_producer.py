import json
from datetime import datetime, timezone
from kafka import KafkaProducer
from api.job_api import fetch_all_jobs

KAFKA_TOPIC  = "jobs_raw"
KAFKA_BROKER = "kafka:29092"


def produce_jobs(pages=5):
    print(f"Producing jobs to Kafka topic: {KAFKA_TOPIC}")

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        api_version=(2, 5, 0),
        request_timeout_ms=30000,
        retries=3,
    )

    jobs = fetch_all_jobs(pages)
    if not jobs:
        raise ValueError("No jobs fetched from API")

    scraped_at = datetime.now(timezone.utc).isoformat()
    for job in jobs:
        job["scraped_at"] = scraped_at          # ← stamp every record
        producer.send(KAFKA_TOPIC, value=job)

    producer.flush()
    producer.close()

    print(f"✅ Produced {len(jobs)} jobs to Kafka topic '{KAFKA_TOPIC}'")
    return len(jobs)