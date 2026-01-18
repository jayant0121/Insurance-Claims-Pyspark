from src.read_data import read_claims_raw
from src.spark_session import get_spark

spark = get_spark()
claims_df = read_claims_raw(
    spark,
    "data/raw/claims_raw"
)

claims_df.printSchema()
claims_df.show(5, truncate=False)
