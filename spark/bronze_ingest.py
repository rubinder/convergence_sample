import argparse

from pyspark.sql import functions as F

from iceberg_conf import spark_session


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--ingest-date", default=None)
    args = ap.parse_args()
    spark = spark_session("bronze_ingest", args.bucket)
    path = f"s3://{args.bucket}/landing/"
    if args.ingest_date:
        path = f"{path}ingest_date={args.ingest_date}/"
    df = spark.read.json(path)
    df = df.withColumn("event_ts", F.to_timestamp("event_ts")).withColumn(
        "ingest_date", F.date_format(F.col("event_ts"), "yyyy-MM-dd")
    )
    cols = [
        "event_id",
        "event_ts",
        "delivery_type",
        "individual_id",
        "household_id",
        "campaign_id",
        "creative_id",
        "segment",
        "network",
        "geo",
        "device",
        "ingest_date",
    ]
    # overwrite (not append) by ingest_date partition so re-ingesting the same
    # landing data is idempotent instead of duplicating rows.
    df.select(*cols).writeTo("glue.convergence.bronze_impressions").overwritePartitions()
    print(f"bronze rows written: {df.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
