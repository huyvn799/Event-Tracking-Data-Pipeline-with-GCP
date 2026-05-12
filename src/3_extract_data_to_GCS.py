import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pymongo import MongoClient
from google.cloud import storage
from datetime import datetime
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus


BUCKET_RAW_DATA_DIR = "raw_data"
OUTPUT_DIR = "output/temp"
LOG_DIR = "output/logs"

load_dotenv()

USERNAME = os.getenv("MONGO_USERNAME") or ""
PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD") or "")
HOST = os.getenv("MONGO_HOST") or "localhost"
PORT = os.getenv("MONGO_PORT") or "27017"
DB_NAME = os.getenv("MONGO_DB_NAME")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

# 1. Cấu hình Logging chi tiết
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(os.path.join(LOG_DIR, "etl_process.log")), logging.StreamHandler()]
)

def export_to_gcs():
    BATCH_SIZE = 100000  # Mỗi batch xử lý 100k dòng để tránh tràn RAM
    
    try:
        # 2. Kết nối MongoDB
        if USERNAME:
            uri = f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/"
        else:
            uri = f"mongodb://{HOST}:{PORT}/"
        client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        total_docs = collection.count_documents({})
        logging.info(f"Bắt đầu Extract. Tổng cộng: {total_docs} dòng.")

        # Kết nối GCS
        with storage.Client() as storage_client:
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
                    
                    # 3. Chuyển đổi sang Table của PyArrow (Tối ưu hơn Pandas đơn thuần)
                    table = pa.Table.from_pandas(df)

                    # Tạo tên file theo ngày và số batch để dễ quản lý
                    current_date = datetime.now().strftime("%Y-%m-%d")
                    file_name = f"{BUCKET_RAW_DATA_DIR}/{current_date}/batch_{batch_count}.parquet"
                    local_path = f"{OUTPUT_DIR}/temp_batch_{batch_count}.parquet"

                    # 4. Convert sang Parquet (Nén Snappy mặc định)[cite: 1]
                    logging.info(f"Đang convert Batch {batch_count} sang Parquet...")
                    pq.write_table(table, local_path, compression='snappy')

                    # 5. Upload lên GCS[cite: 1]
                    logging.info(f"Đang upload {file_name} lên GCS...")
                    blob = bucket.blob(file_name)
                    blob.upload_from_filename(local_path)

                    # Dọn dẹp file tạm và log tiến độ[cite: 1]
                    os.remove(local_path)
                    total_processed += len(batch)
                    logging.info(f"Hoàn thành Batch {batch_count}. Đã xử lý: {total_processed}/{total_docs}")
                    
                    batch = [] # Reset batch

                    break # Thử nghiệm chỉ chạy 1 batch đầu tiên để kiểm tra hệ thống, bỏ break để chạy toàn bộ dữ liệu

            logging.info("--- QUY TRÌNH HOÀN TẤT THÀNH CÔNG ---")

    except Exception as e:
        logging.error(f"LỖI HỆ THỐNG: {str(e)}") # Implement error handling[cite: 1]
    finally:
        client.close()

if __name__ == "__main__":
    export_to_gcs()