import logging
import pathlib
import sys
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import SparkSession, streaming
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    from_json,
    sum,
    to_timestamp,
    window,
)
from pyspark.sql.types import IntegerType, StringType, StructType

project_root = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
import config

watermark_threshold = config.WATERMARK_THRESHOLD_SECONDS
window_duration = config.WINDOW_DURATION


class QueryProgressListener(streaming.StreamingQueryListener):
    def __init__(self):
        self.last_lag = None

    def onQueryStarted(self, event):
        logger = logging.getLogger(__name__)
        logger.info(f"Query started: {event.id}, {event.name}")

    def onQueryProgress(self, event):
        perf_logger = logging.getLogger("performance")
        p = event.progress
        source = p.sources[0]
        lag = float(source.metrics.get("avgOffsetsBehindLatest", 0))
        duration_ms = p.durationMs.get("triggerExecution", "N/A")

        if self.last_lag is None:
            lag_trend = 0.0
            trend_msg = "Baseline lag trend"
        else:
            lag_trend = lag - self.last_lag
            if lag_trend > 0:
                trend_msg = f"Increasing lag trend: {lag_trend}(current lag: {lag})"
            elif lag_trend < 0:
                trend_msg = (
                    f"Decreasing lag trend detected: {lag_trend}(current lag: {lag})"
                )
            else:
                trend_msg = f"No lag trend detected: {lag_trend}(current lag: {lag})"
        self.last_lag = lag

        if lag_trend > 0:
            logger = logging.getLogger(__name__)
            logger.warning(
                f"Increasing lag trend detected: {lag_trend} increase (current lag: {lag})"
            )

        perf_logger.info(
            f"Batch ID: {p.batchId} | Rows Processed: {p.numInputRows} |"
            f" Rate of Processing: {p.processedRowsPerSecond:.1f}/s | Input Rows Per Second: {p.inputRowsPerSecond:.1f}/s |"
            f" Batch Process Duration: {duration_ms}ms | {trend_msg}"
        )

    def onQueryTerminated(self, event):
        logger = logging.getLogger(__name__)
        logger.info(f"Query terminated: {event.id}")


def setup_logging():
    pathlib.Path(project_root / "data/logs/pipeline").mkdir(parents=True, exist_ok=True)
    pathlib.Path(project_root / "data/logs/performance").mkdir(
        parents=True, exist_ok=True
    )

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"{project_root / 'data/logs/pipeline'}/pipeline-{ts}.log"
    performance_log_filename = (
        f"{project_root / 'data/logs/performance'}/performance-{ts}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_filename)],
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)

    perf_handler = logging.FileHandler(performance_log_filename)
    perf_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    perf_logger = logging.getLogger("performance")
    perf_logger.setLevel(logging.INFO)
    perf_logger.addHandler(perf_handler)
    perf_logger.propagate = False


def create_spark_session():
    logger = logging.getLogger(__name__)
    logger.info("Creating Spark session")
    builder = (
        SparkSession.builder.appName(config.SPARK_APP_NAME)
        .master(config.SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
    )

    if config.SPARK_JARS:
        builder = builder.config("spark.jars", config.SPARK_JARS)
    else:
        builder = builder.config("spark.jars.packages", config.SPARK_KAFKA_PACKAGE)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.streams.addListener(QueryProgressListener())
    logger.info("Spark session created successfully")
    return spark


def read_input_data(spark):
    logger = logging.getLogger(__name__)
    logger.info(
        f"Reading input data from Kafka topic '{config.KAFKA_TOPIC}' at {config.KAFKA_BOOTSTRAP_SERVERS}"
    )
    lines = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", config.KAFKA_STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", config.KAFKA_MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", config.KAFKA_FAIL_ON_DATA_LOSS)
        .load()
    )
    result = lines.selectExpr("CAST(value AS STRING) as value")
    logger.info("Input data read successfully from Kafka")
    return result


def process_input_data(lines):
    schema = (
        StructType()
        .add("user_id", StringType(), True)
        .add("event", StringType(), True)
        .add("value", IntegerType(), True)
        .add("timestamp", StringType(), True)
        .add("product_id", StringType(), True)
        .add("category", StringType(), True)
        .add("region", StringType(), True)
        .add("metadata", StructType().add("source", StringType(), True), True)
        .add("corrupt_record", StringType(), True)
    )

    logger = logging.getLogger(__name__)
    logger.info("Processing input data")
    events = lines.select(
        from_json(
            col("value"), schema, {"columnNameOfCorruptRecord": "corrupt_record"}
        ).alias("data")
    ).select("data.*")
    events = events.withColumn("event_timestamp", to_timestamp(col("timestamp")))
    events = events.drop("timestamp")
    events = events.withColumn("ingest_timestamp", current_timestamp())
    logger.info("Input data processed successfully")
    return events, schema


def create_output_paths(name: str):
    path = project_root / f"data/{name}"
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
    return path


def write_raw_output_data(events, output_path_raw=None):
    if output_path_raw is None:
        output_path_raw = create_output_paths("raw")
    logger = logging.getLogger(__name__)
    logger.info("Writing raw output data")
    query = (
        events.writeStream.format("json")
        .outputMode("append")
        .option("path", str(output_path_raw))
        .option("checkpointLocation", str(output_path_raw / "_checkpoint"))
        .start()
    )
    logger.info("Raw output data write started")
    return query


def split_late_events(events, watermark_threshold):
    on_time_events = events.filter(
        (col("ingest_timestamp").cast("long") - col("event_timestamp").cast("long"))
        <= watermark_threshold
    )
    late_events = events.filter(
        (col("ingest_timestamp").cast("long") - col("event_timestamp").cast("long"))
        > watermark_threshold
    )
    return on_time_events, late_events


def validate_events(on_time_events):
    required_columns = [
        "user_id",
        "event",
        "value",
        "product_id",
        "category",
        "region",
        "metadata",
        "event_timestamp",
    ]
    conditions = [col(c).isNotNull() for c in required_columns] + [
        col("corrupt_record").isNull()
    ]
    valid_condition = reduce(lambda a, b: a & b, conditions)
    valid = on_time_events.filter(valid_condition)
    invalid = on_time_events.filter(~valid_condition)
    return valid, invalid


def aggregate_events(valid):
    return (
        valid.withWatermark("event_timestamp", f"{watermark_threshold} seconds")
        .groupBy(
            window(col("event_timestamp"), window_duration),
            "event",
            "category",
            "region",
        )
        .agg(
            sum("value").alias("total_value"),
            count("*").alias("event_count"),
            avg("value").alias("average_value"),
        )
    )


def output_invalid_events(invalid, output_path_invalid=None):
    if output_path_invalid is None:
        output_path_invalid = create_output_paths("invalid")
    logger = logging.getLogger(__name__)
    logger.info("Writing invalid output data")
    query = (
        invalid.writeStream.format("json")
        .outputMode("append")
        .option("path", str(output_path_invalid))
        .option("checkpointLocation", str(output_path_invalid / "_checkpoint"))
        .start()
    )
    logger.info("Invalid output data write started")
    return query


def write_dead_letter_events(late_events, output_path_dead_letter=None):
    if output_path_dead_letter is None:
        output_path_dead_letter = create_output_paths("dead_letter")
    logger = logging.getLogger(__name__)
    logger.info("Writing dead letter output data")
    query = (
        late_events.writeStream.format("json")
        .outputMode("append")
        .option("path", str(output_path_dead_letter))
        .option("checkpointLocation", str(output_path_dead_letter / "_checkpoint"))
        .start()
    )
    logger.info("Dead letter output data write started")
    return query


def write_aggregated_output_data(aggregated_events, output_path_aggregated=None):
    if output_path_aggregated is None:
        output_path_aggregated = create_output_paths("aggregated")
    logger = logging.getLogger(__name__)
    logger.info("Writing aggregated output data")
    query = (
        aggregated_events.coalesce(2)
        .writeStream.format("json")
        .outputMode("append")
        .option("path", str(output_path_aggregated))
        .option("checkpointLocation", str(output_path_aggregated / "_checkpoint"))
        .start()
    )
    logger.info("Aggregated output data write started")
    return query


logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        setup_logging()
        spark = create_spark_session()
        lines = read_input_data(spark)
        events, schema = process_input_data(lines)
        write_raw_output_data(events)
        valid_events, invalid_events = validate_events(events)
        output_invalid_events(invalid_events)
        on_time_events, late_events = split_late_events(
            valid_events, watermark_threshold
        )
        write_dead_letter_events(late_events)
        aggregated_events = aggregate_events(on_time_events)
        write_aggregated_output_data(aggregated_events)
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
    except Exception:
        logger.exception("Pipeline failed or stopped")
    finally:
        for q in spark.streams.active:
            q.stop()
        logger.info("Pipeline execution finished check logs for details")
        logging.shutdown()
