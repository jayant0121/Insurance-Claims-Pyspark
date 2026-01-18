from src.read_data import read_claims_raw
from src.spark_session import get_spark
from src.deduplication import deduplicate_claims_latest

spark = get_spark()
claims_df = read_claims_raw(
    spark,
    "data/raw/claims_raw"
)

claims_latest = deduplicate_claims_latest(claims_df)

claims_latest.printSchema()
claims_latest.show(5, truncate=False)

print("Raw count:", claims_df.count())
print("Latest count:", claims_latest.count())

claims_latest.groupBy("claim_id").count().orderBy(F.desc("count")).show(5)

claims_latest.write\
    .mode("overwrite")\
    .partitionBy("claim_id")\
    .parquet("data/processed/claims_latest")
