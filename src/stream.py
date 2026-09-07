import pathlib
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import streaming
from pyspark.sql.functions import avg, count, split, sum, from_json, col, to_timestamp, current_timestamp, window
from pyspark.sql.types import StructType, StringType, IntegerType


project_root = pathlib.Path(__file__).resolve().parent.parent
watermark_threshold = 30
window_duration = "1 minute"

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
                trend_msg = f"Decreasing lag trend detected: {lag_trend}(current lag: {lag})"
            else:
                trend_msg = f"No lag trend detected: {lag_trend}(current lag: {lag})"
        self.last_lag = lag

        if lag_trend > 0:
            logger = logging.getLogger(__name__)
            logger.warning(f"Increasing lag trend detected: {lag_trend} increase (current lag: {lag})")

        perf_logger.info(
            f"Batch ID: {p.batchId} | Rows Processed: {p.numInputRows} |"
            f" Rate of Processing: {p.processedRowsPerSecond:.1f}/s | Input Rows Per Second: {p.inputRowsPerSecond:.1f}/s |" 
            f" Batch Process Duration: {duration_ms}ms | {trend_msg}"
        )

    def onQueryTerminated(self, event):
        logger = logging.getLogger(__name__)
        logger.info(f"Query terminated: {event.id}")


def setup_logging():
    pathlib.Path(project_root / "data/logs/ingest").mkdir(parents=True, exist_ok=True)
    pathlib.Path(project_root / "data/performance").mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"{project_root / 'data/logs/ingest'}/"f"ingest-{ts}.log"
    performance_log_filename = f"{project_root / 'data/performance'}/"f"performance-{ts}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), 
        logging.FileHandler(log_filename)
        ]
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)

    perf_handler = logging.FileHandler(performance_log_filename)
    perf_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    perf_logger = logging.getLogger("performance")
    perf_logger.setLevel(logging.INFO)
    perf_logger.addHandler(perf_handler)
    perf_logger.propagate = False

def create_spark_session():
    logger = logging.getLogger(__name__)
    logger.info("Creating Spark session")
    spark = SparkSession.builder \
        .appName("Streaming Integration") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    spark.streams.addListener(QueryProgressListener())
    logger.info("Spark session created successfully")
    return spark

def read_input_data(spark):
    logger = logging.getLogger(__name__)
    logger.info("Reading input data from Kafka")
    lines = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "1000") \
        .load()
    result = lines.selectExpr("CAST(value AS STRING) as value")
    logger.info("Input data read successfully from Kafka")
    return result

def process_input_data(lines):
    schema = StructType() \
        .add("user_id", StringType(), True) \
        .add("event", StringType(), True) \
        .add("value", IntegerType(), True) \
        .add("timestamp", StringType(), True) \
        .add("product_id", StringType(), True) \
        .add("category", StringType(), True) \
        .add("region", StringType(), True) \
        .add("metadata", StructType().add("source", StringType(), True), True) \
        .add("corrupt_record", StringType(), True)

    logger = logging.getLogger(__name__)
    logger.info("Processing input data")
    events = lines.select(
        from_json(col("value"), schema, {"columnNameOfCorruptRecord": "corrupt_record"}).alias("data")).select("data.*")
    events = events.withColumn("event_timestamp", to_timestamp(col("timestamp")))
    events = events.drop("timestamp")
    events = events.withColumn("ingest_timestamp", current_timestamp())
    logger.info("Input data processed successfully")
    return events, schema

def create_output_paths(name: str):
    path = project_root / f"data/{name}"
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
    return path

def write_raw_output_data(events, output_path_raw=create_output_paths("raw")):
    logger = logging.getLogger(__name__)
    logger.info("Writing raw output data")
    query = events.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("path", str(output_path_raw)) \
        .option("checkpointLocation", str(output_path_raw / "_checkpoint")) \
        .start()
    logger.info("Raw output data write started")
    return query

def split_late_events(events, watermark_threshold):
    on_time_events = events.filter(
        (col("ingest_timestamp").cast("long") - col("event_timestamp").cast("long")) <= watermark_threshold
    )
    late_events = events.filter(
        (col("ingest_timestamp").cast("long") - col("event_timestamp").cast("long")) > watermark_threshold
    )
    return on_time_events, late_events

def validate_events(on_time_events):
    required_columns = ["user_id", "event", "value", "product_id", "category", "region", "metadata", "event_timestamp"]
    valid_condition = " AND ".join([f"{c} IS NOT NULL" for c in required_columns]) + " AND corrupt_record IS NULL"
    valid = on_time_events.filter(valid_condition)
    invalid = on_time_events.filter(f"NOT ({valid_condition})")
    return valid, invalid

## def debug_invalid_events(invalid):
##    required_columns = ["user_id", "event", "value", "product_id", "category", "region", "metadata", "event_timestamp"]
##    query = invalid.select(*required_columns, "corrupt_record").writeStream \
##        .format("console") \
##        .outputMode("append") \
##        .start()
##    return query


def aggregate_events(valid):
    return valid \
        .withWatermark(
            "event_timestamp", 
            f"{watermark_threshold} seconds") \
        .groupBy(
            window(col("event_timestamp"), window_duration), 
            "event", 
            "category", 
            "region"
            ) \
        .agg(
            sum("value").alias("total_value"),
            count("*").alias("event_count"),
            avg("value").alias("average_value")
        )

def output_invalid_events(invalid, output_path_invalid=create_output_paths("invalid")):
    logger = logging.getLogger(__name__)
    logger.info("Writing invalid output data")
    query = invalid.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("path", str(output_path_invalid)) \
        .option("checkpointLocation", str(output_path_invalid / "_checkpoint")) \
        .start()
    logger.info("Invalid output data write started")
    return query

def write_dead_letter_events(late_events, output_path_dead_letter=create_output_paths("dead_letter")):
    logger = logging.getLogger(__name__)
    logger.info("Writing dead letter output data")
    query = late_events.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("path", str(output_path_dead_letter)) \
        .option("checkpointLocation", str(output_path_dead_letter / "_checkpoint")) \
        .start()
    logger.info("Dead letter output data write started")
    return query

## def late_events(events):
##    return events.filter(
##        (col("ingest_timestamp").cast("long") - col("event_timestamp").cast("long")) > watermark_threshold
##        )


## def write_late_output_data(late_events, output_path_late=OutputPaths.create_output_path_late()):
#    pathlib.Path(output_path_late).mkdir(parents=True, exist_ok=True)
#    logger = logging.getLogger(__name__)
#    logger.info("Writing late output data")
##    query = late_events.writeStream \
##        .format("json") \
##        .option("path", str(output_path_late)) \
##        .option("checkpointLocation", str(output_path_late / "_checkpoint")) \
##        .start()
##    logger.info("Late output data write started")
##    return query

def write_aggregated_output_data(aggregated_events, output_path_aggregated=create_output_paths("aggregated")):
    pathlib.Path(output_path_aggregated).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.info("Writing aggregated output data")
    query = aggregated_events.coalesce(2).writeStream \
        .format("json") \
        .outputMode("append") \
        .option("path", str(output_path_aggregated)) \
        .option("checkpointLocation", str(output_path_aggregated / "_checkpoint")) \
        .start()
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
        on_time_events, late_events = split_late_events(events, watermark_threshold)
        write_dead_letter_events(late_events)
        valid_events, invalid_events = validate_events(on_time_events)
        output_invalid_events(invalid_events)
        aggregated_events = aggregate_events(valid_events)
        ## debug_invalid_events(invalid_events)
        write_aggregated_output_data(aggregated_events)
        ## late_events_data = late_events(events)
        ## write_late_output_data(late_events_data)
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
    except Exception as e:
        logger.exception("Pipeline failed or stopped")
    finally:
        for q in spark.streams.active:
            q.stop()
        logger.info("Pipeline execution finished check logs for details")
        logging.shutdown()