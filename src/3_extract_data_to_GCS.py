import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pymongo import MongoClient
from google.cloud import storage
from datetime import datetime
import time
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
import json
import random

load_dotenv()

USERNAME = os.getenv("MONGO_USERNAME") or ""
PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD") or "")
HOST = os.getenv("MONGO_HOST") or "localhost"
PORT = os.getenv("MONGO_PORT") or "27017"
DB_NAME = os.getenv("MONGO_DB_NAME")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

BUCKET_RAW_DATA_DIR = "raw_data"
BUCKET_IP_LOCATION_DIR = "ip_location"
BUCKET_PRODUCTS_DIR = "products"
OUTPUT_DIR = "output"
OUTPUT_TEMP_DIR = "output/temp"
LOG_DIR = "output/logs"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_TEMP_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# 1. Cấu hình Logging chi tiết
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(os.path.join(LOG_DIR, "etl_process.log")), logging.StreamHandler()]
)

def extract_data_field_from_jsonl(input_folder, output_folder):
    # Liệt kê tất cả file jsonl trong thư mục
    files = [f for f in os.listdir(input_folder) if f.endswith('.jsonl')]
    
    for file_name in files:
        input_path = os.path.join(input_folder, file_name)
        extracted_batch = []
        
        print(f"Đang xử lý file: {file_name}")
        
        with open(input_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    # Parse dòng hiện tại thành dictionary
                    full_record = json.loads(line)
                    
                    # Chỉ lấy giá trị của key 'data'
                    # Sử dụng .get('data') để tránh lỗi nếu dòng đó thiếu key này
                    data_content = full_record.get('data')
                    
                    if data_content is not None:
                        # Nếu data_content là một dict, ta có thể flatten nó sau này
                        extracted_batch.append(data_content)
                        
                except json.JSONDecodeError:
                    print(f"Bỏ qua dòng lỗi định dạng tại file {file_name}")

        # Sau khi trích xuất xong 1 file, chuyển sang DataFrame để chuẩn bị cho Step 1 & 2
        if extracted_batch:
            df = pd.DataFrame(extracted_batch)
            
            # Lưu tạm thành Parquet hoặc xử lý tiếp upload GCS
            output_file = file_name.replace('.jsonl', '.parquet')
            df.to_parquet(os.path.join(output_folder, output_file))
            print(f"Đã trích xuất xong {len(extracted_batch)} bản ghi từ {file_name}")


def clean_document(doc):
    """Xử lý document từ MongoDB để đảm bảo tương thích với Parquet. Chuyển đổi ObjectId và các kiểu dữ liệu phức tạp thành string hoặc JSON."""
    for key, value in doc.items():
        # 1. Xử lý ObjectId
        if isinstance(value, (dict, list)):
            doc[key] = json.dumps(value, ensure_ascii=False)
        else:
            doc[key] = str(value) 

    return doc

def export_db_raw_data_to_gcs(bucket, gcs_folder):
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
        total_docs = collection.estimated_document_count()
        logging.info(f"Bắt đầu Extract. Tổng cộng: {total_docs} dòng.")

        # 3. Extract dữ liệu theo Batch
        cursor = collection.find({}, no_cursor_timeout=True)
        batch = []
        batch_count = 0
        total_processed = 0

        for i, doc in enumerate(cursor):
            # Xử lý ObjectId của MongoDB vì Parquet không đọc được trực tiếp
            doc = clean_document(doc)
            batch.append(doc)

            if len(batch) == BATCH_SIZE or i == total_docs - 1:
                batch_count += 1
                df = pd.DataFrame(batch)
                logging.info(f"Đang xử lý Batch {batch_count} với {len(batch)} dòng...")
                
                # 3. Chuyển đổi sang Table của PyArrow (Tối ưu hơn Pandas đơn thuần)
                table = pa.Table.from_pandas(df)
                logging.info(f"Batch {batch_count} đã chuyển đổi sang Table PyArrow.")

                # Tạo tên file theo ngày và số batch để dễ quản lý
                current_date = datetime.now().strftime("%Y-%m-%d")
                file_name = f"{gcs_folder}/{current_date}/batch_{batch_count}.parquet"
                local_path = f"{OUTPUT_TEMP_DIR}/temp_batch_{batch_count}.parquet"

                # 4. Convert sang Parquet (Nén Snappy mặc định)[cite: 1]
                logging.info(f"Đang convert Batch {batch_count} sang Parquet...")
                pq.write_table(table, local_path, compression='snappy')

                logging.info(f"Batch {batch_count} đã được convert sang Parquet tại {local_path}.")

                # 5. Upload lên GCS[cite: 1]
                logging.info(f"Đang upload {file_name} lên GCS...")
                blob = bucket.blob(file_name)
                blob.upload_from_filename(local_path)

                logging.info(f"Batch {batch_count} đã được upload lên GCS tại {file_name}.")

                # Dọn dẹp file tạm và log tiến độ[cite: 1]
                os.remove(local_path)
                total_processed += len(batch)
                logging.info(f"Hoàn thành Batch {batch_count}. Đã xử lý: {total_processed}/{total_docs}")
                
                batch = [] # Reset batch
                
                logging.info(f"Đang chờ từ 2-5 giây trước khi tiếp tục batch tiếp theo...")
                time.sleep(random.randint(2, 5)) # Tạm dừng 5 giây giữa các batch để tránh quá tải hệ thống và bị rate limit từ GCS
                
        logging.info("--- QUY TRÌNH CHUYỂN RAW DATA LÊN GCSHOÀN TẤT ---")

    except Exception as e:
        logging.error(f"LỖI HỆ THỐNG: {str(e)}") # Implement error handling[cite: 1]
    finally:
        client.close()

def export_file_to_gcs(local_file_path,bucket, gcs_folder):
    try:
        df = pd.read_csv(local_file_path)
        df = df.drop(columns=['Mapped'], errors='ignore')  # Loại bỏ cột 'Mapped' nếu tồn tại
        new_csv_file_path = f"{OUTPUT_TEMP_DIR}/cleaned_ip_locations.csv"
        df.to_csv(new_csv_file_path, index=False)
        file_name = f"{gcs_folder}/ip_locations.csv"
        blob = bucket.blob(file_name)
        blob.upload_from_filename(new_csv_file_path)
        logging.info(f"File ip_location.csv đã được upload lên Bucket {bucket.name} tại {file_name}.")
        os.remove(new_csv_file_path)
    except Exception as e:
        logging.error(f"LỖI KHI UPLOAD IP LOCATION: {str(e)}")

def export_folder_to_gcs(local_folder_path, bucket, gcs_folder):
    try:
        # Liệt kê tất cả các file trong thư mục local
        files = [file_name for file_name in os.listdir(local_folder_path) if file_name.endswith('.jsonl') and os.path.isfile(os.path.join(local_folder_path, file_name))]
    
        logging.info(f"Bắt đầu tải {len(files)} tệp lên bucket {bucket.name}...")

        for file_name in files:
            local_file = os.path.join(local_folder_path, file_name)
            # Đường dẫn trên GCS (blob name)
            blob_path = os.path.join(gcs_folder, file_name).strip("/")
            
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_file)
            
            logging.info(f"Đã tải lên: {os.path.join(gcs_folder, file_name)}")
        
        logging.info(f"Hoàn thành tải {len(files)} tệp lên bucket {bucket.name} tại thư mục {gcs_folder}.")
    except Exception as e:
        logging.error(f"LỖI KHI UPLOAD PRODUCTS: {str(e)}")

def export_to_gcs():
    # Kết nối GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    # Thực hiện export raw data 41m records lên GCS
    # export_db_raw_data_to_gcs(bucket, BUCKET_RAW_DATA_DIR)
    
    # Thực hiện export ip location csv file lên GCS
    ip_location_path = f"{OUTPUT_DIR}/ip_locations.csv"
    # export_file_to_gcs(ip_location_path, bucket, BUCKET_IP_LOCATION_DIR)

    
    # Cách sử dụng
    extract_data_field_from_jsonl(f"{OUTPUT_DIR}/crawl_data", f"{OUTPUT_DIR}/extracted_data_parquet")
    # Thực hiện export products jsonl files lên GCS
    products_folder_path = f"{OUTPUT_DIR}/extracted_data_parquet"
    # export_folder_to_gcs(products_folder_path, bucket, BUCKET_PRODUCTS_DIR)


if __name__ == "__main__":
    export_to_gcs()