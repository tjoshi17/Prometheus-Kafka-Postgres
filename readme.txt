docker compose down -v
docker compose up -d

docker exec -it kafka bash

kafka-topics --create \
  --topic raw-metrics \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

kafka-topics --create \
  --topic processed-metrics \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

python producer\prometheus_to_kafka.py

python processor\kafka_metric_processor.py

docker exec -it postgres psql -U admin -d metricsdb

CREATE TABLE node_metrics (
    instance TEXT,
    metric TEXT,
    value DOUBLE PRECISION,
    timestamp BIGINT,
    source TEXT
);

python processor\kafka_to_postgres.py

SELECT * FROM node_metrics LIMIT 10;

=================================================================