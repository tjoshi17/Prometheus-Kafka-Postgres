import json
import psycopg2
from kafka import KafkaConsumer

# Kafka settings
KAFKA_TOPIC = "processed-metrics"
KAFKA_BROKER = "localhost:9092"

# PostgreSQL settings
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "metricsdb",
    "user": "admin",
    "password": "admin"
}

# Connect to PostgreSQL
conn = psycopg2.connect(**POSTGRES_CONFIG)
cursor = conn.cursor()
print("Connected to PostgreSQL")

# Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Kafka consumer started...")

for message in consumer:
    data = message.value

    try:
        cursor.execute(
            """
            INSERT INTO node_metrics (instance, metric, value, timestamp, source)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                data.get("instance"),
                data.get("metric"),
                data.get("value"),
                data.get("timestamp"),
                data.get("source")
            )
        )

        conn.commit()
        print("Inserted:", data)

    except Exception as e:
        print("Insert failed:", e)
        conn.rollback()