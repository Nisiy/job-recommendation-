from etl.spark_clean_jobs import clean_jobs
import os

if __name__ == "__main__":
    print('Current directory:', os.getcwd())
    
    try:
        cleaned_path = clean_jobs()
        print('✅ SUCCESS!')
        print('Output file:', cleaned_path)
    except Exception as e:
        print('❌ FAILED:', str(e))
        import traceback
        traceback.print_exc()
