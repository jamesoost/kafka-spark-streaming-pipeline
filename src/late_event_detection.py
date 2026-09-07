import pathlib
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import streaming
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType


project_root = pathlib.Path(__file__).resolve().parent.parent
late_threshold = 30

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
        .add("metadata", StringType(), True)

    logger = logging.getLogger(__name__)
    logger.info("Processing input data")
    events = lines.select(from_json(col("value"), schema).alias("data")).select("data.*")
    events = events.withColumn("event_timestamp", to_timestamp(col("timestamp")))
    events = events.drop("timestamp")
    events = events.withColumn("ingest_timestamp", current_timestamp())

    is_stale = (col("ingest_timestamp").cast("long") - col("event_timestamp").cast("long")) > late_threshold

    events = events.withColumn(
        "is_late", 
        (col("event_timestamp").isNull() | is_stale)
        )

    logger.info("Input data processed successfully")
    return events

def create_output_path_raw(base_path_raw=project_root / "data/raw"):
    pathlib.Path(base_path_raw).mkdir(parents=True, exist_ok=True)
    return base_path_raw

def create_output_path_late(base_path_late=project_root / "data/late"):
    pathlib.Path(base_path_late).mkdir(parents=True, exist_ok=True)
    return base_path_late

def write_output_data_raw(events):
    logger = logging.getLogger(__name__)
    raw_path = create_output_path_raw()
    late_path = create_output_path_late()

    def process_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        on_time = batch_df.filter(~col("is_late") )
        late = batch_df.filter(col("is_late"))
        on_time.write.mode("append").option("ignoreNullFields", "false").json(str(raw_path))
        logger.info(f"Written on-time records to {raw_path}")
        late.write.mode("append").option("ignoreNullFields", "false").json(str(late_path))
        logger.info(f"Written late records to {late_path}")
        logger.info(f"Processed batch {batch_id}")

    query = events.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", f"{project_root / 'data/checkpoint'}") \
        .start()
    logger.info("Streaming query for batch processing started")
    query.awaitTermination()
    
    return query

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        setup_logging()
        spark = create_spark_session()
        lines = read_input_data(spark)
        events = process_input_data(lines)
        write_output_data_raw(events)
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
    except Exception as e:
        logger.exception("Pipeline failed or stopped")
    finally:
        logger.info("Pipeline execution finished check logs for details")
        logging.shutdown()