from pyspark.sql import SparkSession

def get_spark():
    return (
        SparkSession.builder
        .appName("InsuranceClaimsAnalytics")
        .config("spark.sql.shuffle.partitions","200")
        .getOrCreate()
    )