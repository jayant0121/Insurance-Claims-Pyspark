from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType,
    TimestampType, DateType
)

claims_schema = StructType([
    StructField("claim_id",IntegerType(),True),
    StructField("policy_id",StringType(),True),
    StructField("customer_id",StringType(),True),
    StructField("updated_at",TimestampType(),True),
    StructField("claim_amount",IntegerType(),True),
    StructField("status",StringType(),False),
    StructField("claim_date",DateType(),True)
])

def read_claims_raw(spark,base_path):
    return (
        spark.read.schema(claims_schema).parquet(base_path)
    )