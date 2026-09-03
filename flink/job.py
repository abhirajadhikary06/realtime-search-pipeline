import json
import requests
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema

def is_valid_event(event_str):
    """Validate incoming Kafka event fields."""
    try:
        data = json.loads(event_str)
        if not data.get("product_id"):
            return False
        if not data.get("name"):
            return False
        if data.get("price", -1) < 0 or data.get("stock", -1) < 0:
            return False
        if data.get("event_type") not in ["UPSERT", "UPDATE_PRICE", "UPDATE_STOCK"]:
            return False
        return True
    except Exception:
        return False

def format_doc(event_str):
    """Normalize payload for Elasticsearch insertion."""
    data = json.loads(event_str)
    return json.dumps({
        "product_id": str(data["product_id"]),
        "name": data["name"],
        "category": data["category"],
        "price": data["price"],
        "stock": data["stock"]
    })

def send_to_elasticsearch(doc_json):
    """Index document directly into Elasticsearch index."""
    try:
        data = json.loads(doc_json)
        doc_id = data["product_id"]
        # Post directly to Elasticsearch running in Docker network
        response = requests.post(
            f"http://elasticsearch:9200/products/_doc/{doc_id}",
            json=data,
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        return f"Indexed product {doc_id} - HTTP {response.status_code}"
    except Exception as e:
        return f"Elasticsearch indexing error: {str(e)}"

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Configure Kafka Source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("product-events") \
        .set_group_id("flink-search-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # DataStream Pipeline: Consume -> Validate -> Normalize -> Index -> Log
    stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source"
    )

    indexed_stream = stream \
        .filter(is_valid_event) \
        .map(format_doc, output_type=Types.STRING()) \
        .map(send_to_elasticsearch, output_type=Types.STRING())

    # Output status to stdout for verification
    indexed_stream.print()

    env.execute("Real-Time Product Search Pipeline")

if __name__ == "__main__":
    main()