import argparse

from pyspark.sql import functions as F, Window

from iceberg_conf import spark_session


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    args = ap.parse_args()
    spark = spark_session("silver_conform", args.bucket)
    b = spark.table("glue.convergence.bronze_impressions")
    w = Window.partitionBy("event_id").orderBy(F.col("event_ts").desc())
    conformed = (
        b.withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
        .withColumn("event_day", F.to_date("event_ts"))
        .select(
            "event_id",
            "event_ts",
            "event_day",
            "delivery_type",
            "individual_id",
            "household_id",
            "campaign_id",
            "segment",
            "network",
            "geo",
            "device",
        )
    )
    conformed.writeTo("glue.convergence.silver_impressions").overwritePartitions()
    print(f"silver rows written: {conformed.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
