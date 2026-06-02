import psycopg2
import os

def get_connection():
    host     = os.getenv('DB_HOST', 'airflow-postgres')
    port     = int(os.getenv('DB_PORT', 5432))
    dbname   = os.getenv('DB_NAME', 'airflow')
    user     = os.getenv('DB_USER', 'airflow')
    password = os.getenv('DB_PASSWORD', 'airflow')
    print(f'Connecting to host={host} port={port} db={dbname}')
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    return conn