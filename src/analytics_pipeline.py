import logging
from src.spark_session import get_spark
from src.customer_analytics import create_enriched_df

# Spark setup
spark = get_spark()
spark.sparkContext.setLogLevel("WARN")

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("analytics-pipeline")

logger.info("Starting customer analytics pipeline")

# Read deduplicated data
logger.info("Reading deduplicated claims data")
claims_latest = spark.read.parquet("data/processed/claims_latest")

logger.info("Enriching claims with customer-level analytics")
claims_enriched = create_enriched_df(claims_latest)


logger.info("Writing enriched claims dataset")
claims_enriched.write \
    .mode("overwrite") \
    .partitionBy("claim_date") \
    .parquet("data/processed/claims_enriched")

logger.info("Customer analytics pipeline completed successfully")