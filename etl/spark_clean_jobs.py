from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, lit, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import psycopg2
import os
from dotenv import load_dotenv

# =========================
# Load environment variables
# =========================
load_dotenv()

# =========================
# Create Spark Session
# =========================
spark = SparkSession.builder \
    .appName("JobCleanTransform") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark connected!")

# =========================
# Define JSON schema
# =========================
schema = StructType([
    StructField("source", StringType(), True),
    StructField("title", StringType(), True),
    StructField("company", StringType(), True),
    StructField("location", StringType(), True),
    StructField("scraped_at", DoubleType(), True),
    StructField("url", StringType(), True),
    StructField("deadline", StringType(), True),
])

# =========================
# Read data from Kafka
# =========================
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "job-listings") \
    .option("startingOffsets", "earliest") \
    .load()

print("✅ Kafka connected!")

# =========================
# Parse Kafka JSON
# =========================
jobs_df = df.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

# =========================
# Clean data
# =========================
cleaned_df = jobs_df \
    .withColumn("title", trim(col("title"))) \
    .withColumn(
        "company",
        when(
            col("company").isNull(),
            lit("Unknown")
        ).otherwise(trim(col("company")))
    ) \
    .withColumn(
        "location",
        when(
            col("location").isNull(),
            lit("Cambodia")
        ).otherwise(trim(col("location")))
    ) \
    .dropDuplicates(["source", "title", "company"])

# =========================
# Count records
# =========================
total = jobs_df.count()
cleaned = cleaned_df.count()

print(f"📦 Total from Kafka: {total}")
print(f"🧹 After cleaning & dedup: {cleaned}")

# =========================
# Collect rows
# =========================
rows = cleaned_df.collect()

# =========================
# PostgreSQL connection
# =========================
conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", 5433)),
    dbname=os.getenv("DB_NAME", "airflow"),
    user=os.getenv("DB_USER", "airflow"),
    password=os.getenv("DB_PASSWORD", "airflow"),
)

cur = conn.cursor()

# =========================
# Insert into PostgreSQL
# =========================
inserted = 0

for row in rows:
    try:
        cur.execute("""
            INSERT INTO job_listings
            (
                source,
                title,
                company,
                location,
                scraped_at,
                url,
                deadline
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                to_timestamp(%s),
                %s,
                %s
            )
            ON CONFLICT DO NOTHING
        """, (
            row.source,
            row.title,
            row.company,
            row.location,
            row.scraped_at,
            row.url,
            row.deadline
        ))

        inserted += 1

    except Exception as e:
        print(f"❌ Skipped row: {e}")

# =========================
# Commit and close
# =========================
conn.commit()

cur.close()
conn.close()

spark.stop()

print(f"✅ Done! {inserted} jobs loaded into PostgreSQL via Spark")