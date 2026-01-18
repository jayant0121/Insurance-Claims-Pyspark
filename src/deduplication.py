from pyspark.sql import functions as F
from pyspark.sql.window import Window

def deduplicate_claims_latest(claims_df):
    window_spec = (
        Window
        .partitionBy(F.col("claim_id"))
        .orderBy(F.col("updated_at").desc())
    )

    return (
        claims_df
        .withColumn("rn",F.row_number().over(window_spec))
        .filter(F.col("rn")==1)
        .drop("rn")
    )