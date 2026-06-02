import os
import glob
import shutil
OUTPUT_PATH = '/opt/airflow/datasource/cleaned/jobs_cleaned.csv'
def run(raw_path=None):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, trim
    if not raw_path:
        raw_path = '/opt/airflow/datasource/raw/jobs_raw.json'
    print(f'Reading: {raw_path}')
    spark = SparkSession.builder \
        .appName('JobsTransform') \
        .master('local[*]') \
        .config('spark.driver.memory', '1g') \
        .config('spark.driver.bindAddress', '0.0.0.0') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    df = spark.read.option('multiline', 'true').json(raw_path)
    print(f'Raw count: {df.count()}')
    df = df.select(
        trim(col('source')).alias('source'),
        trim(col('title')).alias('title'),
        trim(col('company')).alias('company'),
        trim(col('location')).alias('location'),
        trim(col('deadline')).alias('deadline'),
        trim(col('url')).alias('url'),
    )
    df = df.dropna(subset=['title', 'url'])
    df = df.dropDuplicates(['url'])
    df = df.fillna({'company': 'Unknown', 'location': 'Cambodia', 'source': 'Unknown', 'deadline': 'N/A'})
    print(f'Clean count: {df.count()}')
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    tmp_path = '/opt/airflow/datasource/cleaned/_tmp_spark_out'
    df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(tmp_path)
    part_file = glob.glob(f'{tmp_path}/part-*.csv')[0]
    shutil.move(part_file, OUTPUT_PATH)
    shutil.rmtree(tmp_path, ignore_errors=True)
    print(f'Spark transform done -> {OUTPUT_PATH}')
    spark.stop()
    return OUTPUT_PATH
