# Realtime Product Search Pipeline

A distributed, end-to-end streaming search pipeline built with Apache Kafka, Apache Flink, and Elasticsearch. The system ingests streaming product update events, processes and cleanses them in real time using PyFlink, and indexes them into Elasticsearch for instant retrieval and monitoring in Kibana.

---

## Architecture Overview

1. **Producer**: Python application generating real-time product events (UPSERT, UPDATE_PRICE, UPDATE_STOCK) into Apache Kafka.
2. **Message Broker**: Apache Kafka topic (`product-events`) acting as the high-throughput streaming buffer.
3. **Stream Processor**: Apache Flink (PyFlink) consuming Kafka records, validating event schemas, normalizing payloads, and pushing records to Elasticsearch.
4. **Search Engine**: Elasticsearch indexing product records with fast full-text search and analytical capabilities.
5. **Visualization**: Kibana providing data exploration, real-time query analysis (KQL), and metric dashboards.

---

## Project Structure

```text
.
├── elasticsearch/
│   └── index.json          # Elasticsearch index mapping configuration
├── flink/
│   ├── Dockerfile          # Custom PyFlink container setup
│   ├── job.py              # Main PyFlink streaming job logic
│   └── requirements.txt    # Python dependencies for PyFlink
├── producer/
│   ├── producer.py         # Streaming data producer
│   └── requirements.txt    # Producer Python dependencies
├── scripts/
│   └── create-topic.sh     # Shell script to initialize Kafka topics
└── docker-compose.yml      # Orchestration for all pipeline services

```

---

## Prerequisites

* Docker and Docker Compose
* Python 3.10+ (for local host execution, optional)
* `curl` CLI tool

---

## Getting Started

### 1. Start Infrastructure Services

Spin up Kafka, Zookeeper, Flink, Elasticsearch, and Kibana using Docker Compose:

```bash
docker compose up -d

```

Verify that all containers are running and healthy:

```bash
docker ps

```

### 2. Submit the PyFlink Stream Processing Job

Submit the Python job script to the running Flink JobManager container:

```bash
docker exec -it flink-jobmanager flink run -py /opt/flink/usrlib/job.py

```

You can view the job execution graph and processing metrics by navigating to the Flink Web UI at `http://localhost:8081`.

### 3. Run the Event Producer

Start sending synthetic product event streams into Kafka:

```bash
docker compose up producer

```

---

## Verification and Monitoring

### 1. Inspect Flink TaskManager Output

View active logs from the Flink TaskManager to verify incoming records and HTTP indexing statuses:

```bash
docker logs -f flink-taskmanager

```

### 2. Query Processed Data in Elasticsearch

Verify that events have been written to the `products` index in Elasticsearch:

```bash
curl -X GET "localhost:9200/products/_search?pretty"

```

To search for specific categories:

```bash
curl -X GET "localhost:9200/products/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query": {
    "match": {
      "category": "Electronics"
    }
  }
}'

```

### 3. Explore Data in Kibana

1. Open Kibana in your browser at `http://localhost:5601`.
2. Navigate to **Management** > **Stack Management** > **Data Views**.
3. Create a data view named `products` matching the `products` index pattern.
4. Go to **Discover** to query real-time streaming updates using Kibana Query Language (KQL).