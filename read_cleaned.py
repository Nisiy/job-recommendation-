from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
import os

os.environ["PYTHONIOENCODING"] = "utf-8"

spark = SparkSession.builder.appName("ViewCleanedData").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.option("header", True).csv("datasource/cleaned/")

# remove hidden unicode characters
for c in df.columns:
    df = df.withColumn(c, regexp_replace(col(c), "\u200b", ""))

# show only small preview
df.select("*").limit(5).show(truncate=False)

# save clean output
df.coalesce(1).write.mode("overwrite").option("header", True).csv("datasource/output")

print("DONE!")

input("Press ENTER to stop Spark...")