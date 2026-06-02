import json
import os
from api.job_api import fetch_all_jobs


def extract(pages=3):
    try:
        print(f"🚀 Starting extraction for {pages} pages")

        jobs = fetch_all_jobs(pages)

        if not jobs:
            raise ValueError("No jobs returned from API")

        raw_dir = "/opt/airflow/datasource/raw"
        os.makedirs(raw_dir, exist_ok=True)

        raw_path = os.path.join(raw_dir, "jobs_raw.json")

        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(jobs, f, ensure_ascii=False, indent=2)

        print(f"✅ Extract successful — {len(jobs)} jobs → {raw_path}")
        return raw_path

    except Exception as e:
        print(f"❌ Extract failed: {e}")
        raise