import os

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "events")
KAFKA_STARTING_OFFSETS = os.environ.get("KAFKA_STARTING_OFFSETS", "earliest")
KAFKA_MAX_OFFSETS_PER_TRIGGER = os.environ.get("KAFKA_MAX_OFFSETS_PER_TRIGGER", "1000")
# "false" tolerates offsets going missing/reset (e.g. topic recreated in local dev) instead of failing the query
KAFKA_FAIL_ON_DATA_LOSS = os.environ.get("KAFKA_FAIL_ON_DATA_LOSS", "false")

# Spark
SPARK_APP_NAME = os.environ.get("SPARK_APP_NAME", "Streaming Integration")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
SPARK_SHUFFLE_PARTITIONS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4")

SPARK_KAFKA_PACKAGE = os.environ.get(
    "SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
)

SPARK_JARS = os.environ.get("SPARK_JARS")

# Pipeline Variables
WATERMARK_THRESHOLD_SECONDS = int(os.environ.get("WATERMARK_THRESHOLD_SECONDS", "30"))
WINDOW_DURATION = os.environ.get("WINDOW_DURATION", "1 minute")
