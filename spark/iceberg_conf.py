from pyspark.sql import SparkSession


def spark_session(app: str, bucket: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.glue.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config("spark.sql.catalog.glue.warehouse", f"s3://{bucket}/")
        .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .getOrCreate()
    )
