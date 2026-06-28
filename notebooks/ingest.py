import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


DATA_DIR   = "/home/iceberg/data"
CATALOG    = "demo"        
NAMESPACE  = "raw"         
TABLE      = "raw_data"
FULL_TABLE = f"{CATALOG}.{NAMESPACE}.{TABLE}"

spark = (
    SparkSession.builder
    .appName("IcebergDigest")
    .config("spark.sql.defaultCatalog", CATALOG)
    .config("spark.sql.parquet.mergeSchema", "true")
    .config("spark.hadoop.parquet.avro.add-list-element-records", "false")
    .getOrCreate()
)

#---------------------------------------------------------------------------
# Helper to find all .parquet files under a directory
#---------------------------------------------------------------------------

def find_parquet_files(root: str) -> list[str]:
    parquet_files = []
    for dirpath, _, filenames in os.walk(root):
        for filename in filenames:
            if filename.endswith(".parquet"):
                parquet_files.append(os.path.join(dirpath, filename))
    return sorted(parquet_files)

#---------------------------------------------------------------------------
# Load and union all parquet files, aligning columns by name
#---------------------------------------------------------------------------

def union_by_name(files: list[str]) -> DataFrame:
    dfs = []
    for f in files:
        df = spark.read \
            .option("mergeSchema", "true") \
            .parquet(f)
        print(f"  {f}")
        print(f"    columns: {df.columns}")
        dfs.append(df)

    if len(dfs) == 1:
        return dfs[0]


    all_columns = list(dict.fromkeys(
        col for df in dfs for col in df.columns
    ))
    print(f"\nFinal unified columns ({len(all_columns)}): {all_columns}")

    aligned = []
    for df in dfs:
        missing = [col for col in all_columns if col not in df.columns]
        for col in missing:
            df = df.withColumn(col, F.lit(None))
        aligned.append(df.select(all_columns))

    result = aligned[0]
    for df in aligned[1:]:
        result = result.unionByName(df)

    return result

def main():
    if not os.path.isdir(DATA_DIR):
        raise FileNotFoundError(
            f"DATA_DIR '{DATA_DIR}' not found. "
            "Make sure ./data is mounted at /home/iceberg/data in docker-compose."
        )

    parquet_files = find_parquet_files(DATA_DIR)

    if not parquet_files:
        print(f"No .parquet files found under {DATA_DIR}. Nothing to do.")
        spark.stop()
        return

    print(f"\nFound {len(parquet_files)} parquet file(s):\n")
    df = union_by_name(parquet_files)

    print(f"\nTotal rows : {df.count():,}")
    print(f"Schema:")
    df.printSchema()

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    
    #This is for dbt which uses the default namespace
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS default")
    

    print(f"\nWriting to {FULL_TABLE}...")

    df.writeTo(FULL_TABLE) \
        .tableProperty("write.format.default", "parquet") \
        .using("iceberg") \
        .createOrReplace()

    print(f"\n✓ Done. Verifying...")
    spark.sql(f"SELECT * FROM {FULL_TABLE} LIMIT 5").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()