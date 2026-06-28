import sys
from pyspark.sql import SparkSession

CATALOG     = "demo"
NAMESPACE   = "raw"
TABLE       = "raw_data"
FULL_TABLE  = f"{CATALOG}.{NAMESPACE}.{TABLE}"
CATALOG_URI = "http://iceberg-rest:8181"

def get_spark():
    return (
        SparkSession.builder
        .appName("IcebergColumnCast")
        .config("spark.driver.memory",            "1g")
        .config("spark.executor.memory",          "1g")
        .config("spark.executor.cores",           "2")
        .config("spark.ui.enabled",               "false")
        .config("spark.eventLog.enabled",         "false")
        .config("spark.sql.shuffle.partitions",   "4")
        .config("spark.default.parallelism",      "4")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config(f"spark.sql.catalog.{CATALOG}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type",     "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri",      CATALOG_URI)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", CATALOG)
        .getOrCreate()
    )

def remove_last_flight(spark, latest_date, latest_index):
    spark.sql(f"""
        DELETE FROM {FULL_TABLE}
        WHERE date = '{latest_date}'
          AND index = {latest_index}
    """)

    print(f"Deleted last flight row: date={latest_date}, index={latest_index}")


def main():
    spark = get_spark()

    row = spark.sql(f"SELECT MAX(date) AS max_date, MAX(index) AS max_index FROM {FULL_TABLE}").collect()[0]

    latest_date  = row["max_date"]
    latest_index = row["max_index"]

    if latest_date is None:
        print("No existing rows found in the table. Nothing to remove.")
        sys.exit(1)

    print(f"Latest date in table : {latest_date}")
    print(f"Latest index in table: {latest_index}")

    remove_last_flight(spark, latest_date, latest_index)

    print("Remaining rows with latest date after deletion:")
    spark.sql(f"""
        SELECT * FROM {FULL_TABLE}
        WHERE date = '{latest_date}'
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()