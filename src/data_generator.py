from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import Row

def generate_random_data(spark,num_records = 1_000_000):
    base_df = spark.range(0,num_records)

    claims_df = (
        base_df.withColumn("claim_id",(F.col("id")%200_000).cast("int"))
        .withColumn("policy_id",F.concat(F.lit("P"),(F.col("id")%50_000)))
        .withColumn("customer_id",F.concat(F.lit("C"),(F.col("id")%20_000)))
        .withColumn(
            "claim_date",
            F.expr("date_add(to_date('2023-01-01'),CAST(id%365 AS INT))")
        )
        .withColumn(
            "updated_at",
            (
                F.col("claim_date").cast("timestamp")+
                F.expr("INTERVAL 1 HOUR")*(F.col("id")%240)
            )
        )
        .withColumn(
            "claim_amount",
            (F.rand()*10000+500).cast("int")
        )
        .withColumn(
            "status",
            F.when(F.col("id")%3==0,"OPEN")
            .when(F.col("id")%3==1,"APPROVED")
            .otherwise("REJECTED")
        )
        .drop("id")
    )
    return claims_df


