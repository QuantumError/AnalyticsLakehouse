from pyspark.sql import SparkSession

CATALOG     = "demo"
NAMESPACE   = "raw"
TABLE       = "raw_data"
FULL_TABLE  = f"{CATALOG}.{NAMESPACE}.{TABLE}"
CATALOG_URI = "http://iceberg-rest:8181"

spark = (
    SparkSession.builder
    .appName("AddPartitionField")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .config(f"spark.sql.catalog.{CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.type",      "rest")
    .config(f"spark.sql.catalog.{CATALOG}.uri",       CATALOG_URI)
    .config(f"spark.sql.catalog.{CATALOG}.warehouse",  CATALOG)
    .getOrCreate()
)

spark.sql(f"""
    ALTER TABLE {FULL_TABLE}
    ADD PARTITION FIELD DAYS(date)
""")

print(f"Partition field DAYS(date) added successfully to {FULL_TABLE}.")

spark.stop()