import time
import json
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

PROMETHEUS_URL = "http://localhost:9090/api/v1/query"
KAFKA_BROKER = "localhost:9092"
TOPIC = "raw-metrics"

# metrics to keep (VERY IMPORTANT for cardinality control)
METRICS = [
    "node_cpu_seconds_total",
    "node_memory_MemAvailable_bytes",
    "node_filesystem_size_bytes"
]

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(0, 10, 1),
    retries=10,
    request_timeout_ms=90000,
    max_in_flight_requests_per_connection=1
)

print("Warming up Kafka producer...")
time.sleep(5)
producer.bootstrap_connected()
print("Kafka producer ready")

def query_prometheus(metric):
    try:
        response = requests.get(
            PROMETHEUS_URL,
            params={"query": metric},
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Prometheus query failed for {metric}: {e}")
        return {"status": "error"}

def safe_send(message):
    try:
        future = producer.send(TOPIC, message)
        future.get(timeout=60)  # force delivery confirmation
    except KafkaError as e:
        print("Kafka send failed:", e)

def process_and_send():
    for metric in METRICS:
        data = query_prometheus(metric)

        if data["status"] != "success":
            continue

        results = data["data"]["result"]

        for r in results:
            message = {
                "metric": metric,
                "instance": r["metric"].get("instance"),
                "value": float(r["value"][1]),
                "timestamp": int(float(r["value"][0]))
            }

            safe_send(message)
            print("Sent:", message)
            time.sleep(0.05)

if __name__ == "__main__":
    print("Waiting for services to stabilize...")
    time.sleep(10)

    while True:
        process_and_send()
        time.sleep(15)