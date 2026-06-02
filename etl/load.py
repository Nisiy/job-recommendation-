import os
import glob
import pandas as pd
from database.connection import get_connection

def load(cleaned_path=None):
    if cleaned_path is None or not os.path.exists(cleaned_path):
        cleaned_dir = '/opt/airflow/datasource/cleaned'
        files = glob.glob(os.path.join(cleaned_dir, '*.csv'))
        if not files:
            raise FileNotFoundError('No cleaned CSV found')
        cleaned_path = max(files, key=os.path.getctime)
    print(f'Loading: {cleaned_path}')
    df = pd.read_csv(cleaned_path)
    df = df.where(pd.notnull(df), None)
    print(f'Rows: {len(df)}')
    if df.empty:
        raise ValueError('CSV is empty')
    conn = get_connection()
    cur = conn.cursor()
    inserted = 0
    skipped = 0
    sql = 'INSERT INTO job_listings (source, title, company, location, url, deadline) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (url) DO NOTHING'
    for _, row in df.iterrows():
        try:
            cur.execute(sql, (row.get('source'), row.get('title'), row.get('company'), row.get('location'), row.get('url'), row.get('deadline')))
            conn.commit()
            inserted += 1
        except Exception as e:
            conn.rollback()
            print(f'Row skipped: {e}')
            skipped += 1
    cur.close()
    conn.close()
    print(f'Done! Inserted: {inserted} | Skipped: {skipped}')

if __name__ == '__main__':
    load()