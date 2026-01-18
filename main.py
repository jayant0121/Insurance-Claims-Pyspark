from pyspark.sql import functions as F
from src.spark_session import get_spark
from src.data_generator import generate_random_data

spark = get_spark()

claims_raw = generate_random_data(spark, num_records=1_000_000)
claims_raw.printSchema()
claims_raw.count()
claims_raw.select("claim_id").distinct().count()
claims_raw.groupBy("claim_id").count().orderBy(F.desc("count")).show(5)


claims_raw.write.mode("overwrite").partitionBy("claim_date").parquet("data/raw/claims_raw")

