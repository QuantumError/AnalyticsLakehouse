import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import timedelta

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

def insert_new_flight(spark, new_date, new_index):
    new_row = Row(
        month                    = new_date.month,
        day_of_month             = new_date.day,
        actual_departure_time    = 1196,
        scheduled_departure_time = 1195,
        actual_arrival_time      = 1302,
        scheduled_arrival_time   = 1310,
        unique_carrier           = "WN",
        flight_number            = 609,
        plane_tail_number        = None,   
        actual_elapsed_time      = 46,
        scheduled_elapsed_time   = 55,
        air_time                 = 37,
        arrival_delay            = -8,
        departure_delay          = 1,
        origin                   = "ABQ",
        dest                     = "AMA",
        distance                 = "277",
        taxi_in                  = 3,
        taxi_out                 = 6,
        cancelled                = 0,
        cancellation_code        = None,    
        diverted                 = 0,
        carrier_delay            = None,    
        weather_delay            = None,    
        nas_delay                = None,    
        security_delay           = None,    
        late_aircraft_delay      = None,    
        date                     = new_date,
        index                    = new_index,
    )

    schema = spark.table(FULL_TABLE).schema
    new_df = spark.createDataFrame([new_row], schema=schema)

    new_df.writeTo(FULL_TABLE).append()

    print(f"Inserted new flight row: date={new_date}, index={new_index}")


def main():
    spark = get_spark()

    row_date = spark.sql(f"SELECT MAX(date) AS max_date FROM {FULL_TABLE}").collect()[0]

    latest_date = row_date["max_date"] 
    if latest_date is None:
        print("No existing rows found in the table. Cannot determine latest date.")
        sys.exit(1)

    new_date  = latest_date + timedelta(days=1)

    row_index = spark.sql(f"SELECT MAX(index) AS max_index FROM {FULL_TABLE}").collect()[0]
    new_index = (row_index["max_index"] or 0) + 1

    print(f"Latest date in table : {latest_date}")
    print(f"New row date         : {new_date}")
    print(f"New row index        : {new_index}")

    insert_new_flight(spark, new_date, new_index)

    spark.sql(f"""
        SELECT * FROM {FULL_TABLE}
        WHERE date = '{new_date}'
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()