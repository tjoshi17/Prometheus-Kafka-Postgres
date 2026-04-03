import json
from kafka import KafkaConsumer, KafkaProducer

RAW_TOPIC = "raw-metrics"
PROCESSED_TOPIC = "processed-metrics"
KAFKA_BROKER = "localhost:9092"

# Only keep these metrics (fine-tuning logic)
ALLOWED_METRICS = {
    "node_cpu_seconds_total",
    "node_memory_MemAvailable_bytes"
}

consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=None, 
    consumer_timeout_ms=1000,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("🚀 Metric processor started...")

for msg in consumer:
    data = msg.value

    # ✅ Filter metrics
    if data["metric"] not in ALLOWED_METRICS:
        continue

    # ✅ Transform schema (example enrichment)
    processed = {
        "instance": data["instance"],
        "metric": data["metric"],
        "value": round(data["value"], 4),
        "timestamp": data["timestamp"],
        "source": "prometheus"
    }

    producer.send(PROCESSED_TOPIC, processed)
    print("Processed:", processed)