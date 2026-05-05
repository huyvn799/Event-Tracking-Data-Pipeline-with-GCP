import logging
import pandas as pd
from pymongo import MongoClient
from google.cloud import storage
from datetime import datetime
import os

# 1. Cấu hình Logging chi tiết
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("etl_process.log"), logging.StreamHandler()]
)

def export_to_gcs():
    # Thông số cấu hình
    MONGO_URI = "mongodb://localhost:27017/"
    DB_NAME = "your_database"
    COLLECTION_NAME = "your_collection"
    GCS_BUCKET_NAME = "your-gcs-bucket-name"
    BATCH_SIZE = 100000  # Mỗi batch xử lý 100k dòng để tránh tràn RAM
    
    try:
        # 2. Kết nối MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        total_docs = collection.count_documents({})
        logging.info(f"Bắt đầu Extract. Tổng cộng: {total_docs} dòng.")

        # Kết nối GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)

        # 3. Extract dữ liệu theo Batch
        cursor = collection.find({})
        batch = []
        batch_count = 0
        total_processed = 0

        for i, doc in enumerate(cursor):
            # Xử lý ObjectId của MongoDB vì Parquet không đọc được trực tiếp
            doc['_id'] = str(doc['_id'])
            batch.append(doc)

            if len(batch) == BATCH_SIZE or i == total_docs - 1:
                batch_count += 1
                df = pd.DataFrame(batch)
                
                # Tạo tên file theo ngày và số batch để dễ quản lý
                current_date = datetime.now().strftime("%Y-%m-%d")
                file_name = f"raw_data/{current_date}/batch_{batch_count}.parquet"
                local_path = f"temp_batch_{batch_count}.parquet"

                # 4. Convert sang Parquet (Nén Snappy mặc định)[cite: 1]
                logging.info(f"Đang convert Batch {batch_count} sang Parquet...")
                df.to_parquet(local_path, index=False)

                # 5. Upload lên GCS[cite: 1]
                logging.info(f"Đang upload {file_name} lên GCS...")
                blob = bucket.blob(file_name)
                blob.upload_from_filename(local_path)

                # Dọn dẹp file tạm và log tiến độ[cite: 1]
                os.remove(local_path)
                total_processed += len(batch)
                logging.info(f"Hoàn thành Batch {batch_count}. Đã xử lý: {total_processed}/{total_docs}")
                
                batch = [] # Reset batch

        logging.info("--- QUY TRÌNH HOÀN TẤT THÀNH CÔNG ---")

    except Exception as e:
        logging.error(f"LỖI HỆ THỐNG: {str(e)}") # Implement error handling[cite: 1]
    finally:
        client.close()

if __name__ == "__main__":
    export_to_gcs()