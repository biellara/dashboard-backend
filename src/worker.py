import time
from src.infrastructure.ingestion.bulk_processor import process_pending_uploads

if __name__ == "__main__":
    print("🚀 ETL Worker iniciado...")

    while True:
        process_pending_uploads()
        time.sleep(10)