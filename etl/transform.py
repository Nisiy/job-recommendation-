import os
import glob
import shutil

BASE_DIR    = "/opt/airflow"
OUTPUT_PATH = os.path.join(BASE_DIR, "datasource/cleaned/jobs_cleaned.csv")
TMP_PATH    = os.path.join(BASE_DIR, "datasource/cleaned/_tmp_spark")


def run(raw_path=None):
    """Entry point called by both the DAG (PythonOperator) and __main__."""

    if raw_path is None:
        raw_path = os.path.join(BASE_DIR, "datasource/raw/jobs_raw.json")

    raw_path = os.path.abspath(raw_path)

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw JSON not found: {raw_path}")

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, trim, when, lit

    spark = (
        SparkSession.builder
        .appName("CambodiaJobsTransform")
        .master("spark://spark-master:7077")
        .config("spark.driver.host", os.getenv("AIRFLOW_CONTAINER_HOST", "airflow-scheduler"))
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"📥 Reading: {raw_path}")
    df = spark.read.option("multiline", "true").json(raw_path)
    raw_count = df.count()
    print(f"📊 Raw count: {raw_count}")

    if raw_count == 0:
        spark.stop()
        raise ValueError(f"Raw file is empty: {raw_path}")

    df = df.select(
        trim(col("source")).alias("source"),
        trim(col("title")).alias("title"),
        when(col("company").isNull(), lit("Unknown"))
            .otherwise(trim(col("company"))).alias("company"),
        when(col("location").isNull(), lit("Cambodia"))
            .otherwise(trim(col("location"))).alias("location"),
        col("scraped_at"),
        trim(col("url")).alias("url"),
        trim(col("deadline")).alias("deadline"),
    )

    df = df.dropna(subset=["title"])
    df = df.dropDuplicates(["source", "title", "company"])

    clean_count = df.count()
    print(f"🧹 Clean count: {clean_count}")

    if clean_count == 0:
        spark.stop()
        raise ValueError("Spark produced 0 rows after cleaning — check raw data quality")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    shutil.rmtree(TMP_PATH, ignore_errors=True)

    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(TMP_PATH)

    part_files = glob.glob(f"{TMP_PATH}/part-*.csv")
    if not part_files:
        spark.stop()
        raise FileNotFoundError("Spark wrote no part file — check executor logs")

    shutil.move(part_files[0], OUTPUT_PATH)
    shutil.rmtree(TMP_PATH, ignore_errors=True)

    spark.stop()
    print(f"✅ Transform done → {OUTPUT_PATH}")
    return OUTPUT_PATH


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Spark jobs transform standalone")
    parser.add_argument(
        "--raw_path",
        default=os.path.join(BASE_DIR, "datasource/raw/jobs_raw.json"),
        help="Path to raw JSON file",
    )
    args = parser.parse_args()
    result = run(raw_path=args.raw_path)
    print(f"Done: {result}")