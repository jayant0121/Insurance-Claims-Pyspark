import logging
from src.read_data import read_claims_raw
from src.spark_session import get_spark
from src.deduplication import deduplicate_claims_latest

# Spark setup
spark = get_spark()
spark.sparkContext.setLogLevel("WARN")

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("dedup-pipeline")

logger.info("Starting claims deduplication pipeline")

# Read raw data
logger.info("Reading raw claims data")
claims_df = read_claims_raw(
    spark,
    "data/raw/claims_raw"
)

# Deduplicate 
logger.info("Deduplicating claims to get latest records")
claims_latest = deduplicate_claims_latest(claims_df)

logger.info("Writing latest claims dataset") 

claims_latest.write.mode("overwrite").partitionBy("claim_date").parquet("data/processed/claims_latest") 

row_count = claims_latest.count()
logger.info("Claims deduplication completed. Output row count: %d", row_count)