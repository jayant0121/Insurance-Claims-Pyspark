from pyspark.sql import functions as F
from pyspark.sql.window import Window as Window

def create_enriched_df(spark, claims_latest):
    customer_window = (
        Window.partitionBy(F.col("customer_id"))
    )
    customer_window_time = (
        Window.partitionBy(F.col("customer_id")).orderBy("claim_date")
    )

    enriched_df = (
        claims_latest
        .withColumn(
            "total_claims_by_customer",
            F.count("*").over("customer_window")
        )
        .withColumn(
            "total_claim_amount_by_customer",
            F.sum("claim_amount").over("customer_window")
        )
        .withColumn(
            "prev_claim_amount",
            F.lag("claim_amount").over("customer_window_time")
        )
        .withColumn(
            "days_since_prev_claim",
            F.date_diff(
                F.col("claim_date"),
                F.lag("claim_amount").over("customer_window_time")
            )
        )
    )
    return enriched_df