from etl.extract import extract
from etl.transform import transform
from etl.load import load

def run_pipeline(pages=3):
    print("Starting Cambodia Jobs Pipeline...")
    
    raw_path = extract(pages)
    print(f"Extract done → {raw_path}")

    cleaned_path = transform(raw_path)
    print(f"Transform done → {cleaned_path}")

    load(cleaned_path)
    print("Load done → Pipeline complete!")