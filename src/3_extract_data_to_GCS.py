import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pymongo import MongoClient
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime
import time
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
import json
import random
import genson

load_dotenv()

USERNAME = os.getenv("MONGO_USERNAME") or ""
PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD") or "")
HOST = os.getenv("MONGO_HOST") or "localhost"
PORT = os.getenv("MONGO_PORT") or "27017"
DB_NAME = os.getenv("MONGO_DB_NAME")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

BUCKET_RAW_DATA_DIR = "bronze/summary_2"
BUCKET_IP_LOCATION_DIR = "bronze/ip_location"
BUCKET_PRODUCTS_DIR = "bronze/products_jsonl"
OUTPUT_DIR = "output"
OUTPUT_TEMP_DIR = "output/temp"
LOG_DIR = "output/logs"
SUMMARY_JSONL_DIR = "output/summary_jsonl2" # Thư mục tạm lưu file JSONL trên máy ảo
MICRO_BATCH_SIZE = 5000     # Số dòng gom trong RAM trước khi ghi xuống ổ cứng
RECORDS_PER_FILE = 100000   # Số dòng tối đa của 1 file JSONL (Ngưỡng phân mảnh)

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_TEMP_DIR, exist_ok=True)
os.makedirs(SUMMARY_JSONL_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# 1. Cấu hình Logging chi tiết
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(os.path.join(LOG_DIR, "etl_process.log")), logging.StreamHandler()]
)

def get_data_dictionary_from_db():
    try:
        # 2. Kết nối MongoDB
        if USERNAME:
            uri = f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/"
        else:
            uri = f"mongodb://{HOST}:{PORT}/"
        client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # 1. Lấy mẫu ngẫu nhiên 100,000 dòng để phân tích schema
        pipeline = [{"$sample": {"size": 100000}}]
        cursor = collection.aggregate(pipeline)

        builder = genson.SchemaBuilder()

        print("[*] Đang phân tích schema từ tập mẫu...")
        for doc in cursor:
            # Xóa trường _id vì dòng nào cũng có và nó là duy nhất
            doc.pop('_id', None)
            builder.add_object(doc)

        # 2. Xuất ra JSON Schema chuẩn
        schema = builder.to_schema()
        with open("summary_schema.json", "w") as f:
            json.dump(schema, f, indent=4)
        print("[SUCCESS] Đã tạo thành công file glamira_schema.json!")

    except Exception as e:
        logging.info(f"Lỗi ở quá trình lấy data dictionary: {e}")

def stringify_document(doc):
    """Xử lý document từ MongoDB để đảm bảo tương thích với Parquet. Chuyển đổi ObjectId và các kiểu dữ liệu phức tạp thành string hoặc JSON."""
    for key, value in doc.items():
        # 1. Xử lý ObjectId
        if isinstance(value, (dict, list)):
            doc[key] = json.dumps(value, ensure_ascii=False)
        else:
            doc[key] = str(value)

    return doc

def upload_to_gcs(local_file_path, gcs_blob_name):
    """Hàm xử lý upload file từ máy ảo lên GCS"""
    print(f"[*] Đang upload {local_file_path} lên GCS: {gcs_blob_name}...")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(gcs_blob_name)
        
        blob.upload_from_filename(local_file_path)
        print(f"[SUCCESS] Đã upload xong {gcs_blob_name}")
        return True
    except Exception as e:
        print(f"[ERROR] Upload GCS thất bại: {e}")
        return False

def export_raw_data_by_parquet_to_gcs(bucket, gcs_folder):
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
            doc = stringify_document(doc)
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

                if upload_to_gcs(local_path, file_name):
                    logging.info(f"Batch {batch_count} đã được upload lên GCS tại {file_name}.")
                    # Dọn dẹp file tạm và log tiến độ[cite: 1]
                    os.remove(local_path)
                    total_processed += len(batch)
                    logging.info(f"Hoàn thành Batch {batch_count}. Đã xử lý: {total_processed}/{total_docs}")
                else:
                    logging.info(f"Không upload được {local_path} lên Bucket {file_name}")
                
                batch = [] # Reset batch
                
                logging.info(f"Đang chờ từ 2-5 giây trước khi tiếp tục batch tiếp theo...")
                time.sleep(random.randint(2, 5)) # Tạm dừng 5 giây giữa các batch để tránh quá tải hệ thống và bị rate limit từ GCS
                
        logging.info("--- QUY TRÌNH CHUYỂN RAW DATA LÊN GCSHOÀN TẤT ---")

    except Exception as e:
        logging.error(f"LỖI HỆ THỐNG: {str(e)}") # Implement error handling[cite: 1]
    finally:
        cursor.close()
        client.close()

def standardize_document(doc):
    # Đổi kiểu string cho object_id
    if '_id' in doc: doc['_id'] = str(doc['_id'])
    # Quy tắc 1: Đồng nhất hóa trường option (Ép Object thành Array 1 phần tử)
    if 'option' in doc:
        opt_data = doc["option"]
        # Cách 1: Tách option thành 2 cột riêng option_object và option_list vì ý nghĩa khác nhau
        if isinstance(opt_data, dict):
            # Tình huống 1: Là Object -> Chuyển sang cột riêng cho Object
            doc["option_object"] = opt_data
            doc["option_list"] = [] # Để mảng rỗng để bảo toàn schema
            
        elif isinstance(opt_data, list):
            # Tình huống 2: Là Array -> Chuyển sang cột riêng cho Array
            doc["option_list"] = opt_data
            doc["option_object"] = None

        # Cách 2: Nếu option là object -> Đưa object vào trong mảng -> mảng 1 object
                # Nếu option là mảng thì giữ nguyên
        if isinstance(opt_data, dict):
            doc['option'] = [doc['option']]
        elif doc['option'] is None or not isinstance(opt_data, list):
            doc['option'] = []

    # Quy tắc 2: Xử lý mảng cart_products nếu bị null và option của từng item
    if 'cart_products' in doc:
        if isinstance(doc['cart_products'], list):
            # Duyệt qua từng sản phẩm con bên trong giỏ hàng
            for product in doc["cart_products"]:
                # Kiểm tra xem sản phẩm con này có trường option hay không
                if "option" in product:
                    opt_cart_data = product["option"]
                    
                    # TÌNH HUỐNG 1: option là None hoặc một chuỗi ký tự (toàn bộ là chuỗi rỗng "") -> []
                    if opt_cart_data is None or isinstance(opt_cart_data, str):
                        product["option"] = []
                    # TÌNH HUỐNG 3: option là một Object đơn lẻ (dict) thay vì mảng
                    elif isinstance(opt_cart_data, dict):
                        # Bọc nó lại thành một mảng chứa 1 phần tử duy nhất
                        product["option"] = [opt_cart_data]
                    # TÌNH HUỐNG 3: Nếu đã là list sẵn rồi thì giữ nguyên
                    elif isinstance(opt_cart_data, list):
                        pass

        if doc['cart_products'] is None or not isinstance(doc['cart_products'], list):
            doc['cart_products'] = []
        
    
    return doc

def export_raw_data_to_jsonl(micro_batch_size = MICRO_BATCH_SIZE, records_per_file = RECORDS_PER_FILE):
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
        file_index = 1
        current_file_records = 0
        ram_buffer = [] # Micro-batch buffer nằm trong RAM
        total_processed = 0

        print("[*] Bắt đầu quét và phân mảnh dữ liệu từ MongoDB...")
        start_export_time = time.perf_counter()
        start_batch_time = time.perf_counter()
        for i, doc in enumerate(cursor):
            # Xử lý các field dạng object, array, object_id như option, cart_products, _id
            # để bigquery nhận diện đúng
            doc = standardize_document(doc)
            json_str = json.dumps(doc, ensure_ascii=False).replace("\n", " ").replace("\r", " ")
            
            ram_buffer.append(json_str)
            total_processed += 1
            current_file_records += 1

            # TÌNH HUỐNG 1: Bộ đệm RAM đủ 5,000 dòng -> Ghi cuốn chiếu xuống đĩa cứng
            if len(ram_buffer) == MICRO_BATCH_SIZE or i == total_docs - 1:
                file_name = f"summary_part_{file_index:03d}.jsonl"
                file_path = os.path.join(SUMMARY_JSONL_DIR, file_name)
                
                # Sử dụng mode "a" (Append) để ghi nối tiếp cực kỳ an toàn
                with open(file_path, "a", encoding="utf-8") as f:
                    f.write("\n".join(ram_buffer) + "\n")
                    
                ram_buffer = [] # Xóa sạch bộ đệm RAM để giải phóng bộ nhớ ngay lập tức

            # TÌNH HUỐNG 2: File hiện tại đã tích lũy đủ 100,000 dòng -> Chuyển qua file mới
            if current_file_records >= RECORDS_PER_FILE:
                print(f"[+] Đã hoàn thành đóng gói file phân mảnh: summary_part_{file_index:03d}.jsonl trong {time.perf_counter() - start_batch_time}")
                file_index += 1
                current_file_records = 0 # Reset bộ đếm dòng của file mới
                time.sleep(random.randint(2, 5)) # Tạm dừng 5 giây giữa các batch để tránh quá tải hệ thống và bị rate limit từ GCS
                
                start_batch_time = time.perf_counter() # Tính start_time của batch mới
            
        logging.info(f"\n[SUCCESS] Hoàn thành xuất sắc! Tổng cộng trích xuất thành công {total_processed} records trong {time.perf_counter() - start_export_time}")

    except Exception as e:
        logging.error(f"[CRITICAL ERROR] LỖI HỆ THỐNG: {str(e)}")
    finally:
        cursor.close()
        client.close()

def process_raw_jsonl_to_gcs(local_folder_path, bucket, gcs_folder):
    try:
        # Liệt kê tất cả các file trong thư mục local
        files = [file_name for file_name in os.listdir(local_folder_path) if file_name.endswith('.jsonl') and os.path.isfile(os.path.join(local_folder_path, file_name))]
    
        logging.info(f"Bắt đầu tải {len(files)} tệp lên bucket {bucket.name}...")

        for file_name in files:
            local_file = os.path.join(local_folder_path, file_name)
            # Đường dẫn trên GCS (blob name)
            blob_path = os.path.join(gcs_folder, file_name).strip("/")
            
            if upload_to_gcs(local_file, blob_path):
                logging.info(f"Đã tải lên: {os.path.join(gcs_folder, file_name)}")
            else:
                logging.info(f"Không tải được {file_name} lên Bucket {gcs_folder}")
        
        logging.info(f"Hoàn thành tải {len(files)} tệp lên bucket {bucket.name} tại thư mục {gcs_folder}.")
    except Exception as e:
        logging.error(f"LỖI KHI UPLOAD PRODUCTS: {str(e)}")

def process_ip_location_to_gcs(local_file_path,bucket, gcs_folder):
    try:
        df = pd.read_csv(local_file_path)
        df = df.drop(columns=['Mapped'], errors='ignore')  # Loại bỏ cột 'Mapped' nếu tồn tại
        new_csv_file_path = f"{OUTPUT_TEMP_DIR}/cleaned_ip_locations.csv"
        df.to_csv(new_csv_file_path, index=False)
        file_name = f"{gcs_folder}/ip_locations.csv"
        if upload_to_gcs(new_csv_file_path, file_name):
            logging.info(f"File ip_locations.csv đã được upload lên Bucket {bucket.name} với tên {file_name}")
            os.remove(new_csv_file_path)
        else:
            logging.info(f"Không upload được ip_locations.csv lên Bucket {bucket.name}")

    except Exception as e:
        logging.error(f"LỖI KHI UPLOAD IP LOCATION: {str(e)}")

def extract_product_data(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(f"{OUTPUT_DIR}/extracted_data_jsonl", exist_ok=True)

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
                        extracted_batch.append(data_content)
                        
                except Exception as e:
                    print(f"[ERROR] khi thực hiện parse json sang object tại file {file_name}")

        # Sau khi trích xuất xong 1 file, ghi lại vào file jsonl mới
        if extracted_batch:
            output_path = f"{output_folder}/{file_name}"
            with open(output_path, "w", encoding="utf-8") as f:
                for item in extracted_batch:
                    # ensure_ascii=False giúp giữ nguyên chữ tiếng Việt có dấu, không bị biến thành \u1234
                    json_line = json.dumps(item, ensure_ascii=False).replace("\n", " ").replace("\r", " ")
                    
                    # Ghi chuỗi JSON và thêm dấu xuống dòng \n ở cuối
                    f.write(json_line + "\n")
            print(f"[SUCCESS] Đã trích xuất xong {len(extracted_batch)} bản ghi từ {file_name}")

def process_products_to_gcs(local_folder_path, bucket, gcs_folder):
    try:
        # Liệt kê tất cả các file trong thư mục local
        files = [file_name for file_name in os.listdir(local_folder_path) if file_name.endswith('.jsonl') and os.path.isfile(os.path.join(local_folder_path, file_name))]
    
        logging.info(f"Bắt đầu tải {len(files)} tệp lên bucket {bucket.name}...")

        for file_name in files:
            local_file = os.path.join(local_folder_path, file_name)
            # Đường dẫn trên GCS (blob name)
            blob_path = os.path.join(gcs_folder, file_name).strip("/")
            
            if upload_to_gcs(local_file, blob_path):
                logging.info(f"Đã tải lên: {os.path.join(gcs_folder, file_name)}")
            else:
                logging.info(f"Không tải được {file_name} lên Bucket {gcs_folder}")
        
        logging.info(f"Hoàn thành tải {len(files)} tệp lên bucket {bucket.name} tại thư mục {gcs_folder}.")
    except Exception as e:
        logging.error(f"LỖI KHI UPLOAD PRODUCTS: {str(e)}")

def export_to_gcs():
    # Kết nối GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    # 1. Thực hiện export ip location csv file lên GCS
    # ip_location_path = f"{OUTPUT_DIR}/ip_locations.csv"
    # process_ip_location_to_gcs(ip_location_path, bucket, BUCKET_IP_LOCATION_DIR)

    # 2. Thực hiện export products jsonl files lên GCS
    # products_jsonl_path = f"{OUTPUT_DIR}/bronze_product_jsonl"
    # Chỉ lấy field data trong crawl_product
    # extract_product_data("output/crawl_data",products_jsonl_path)
    # process_products_to_gcs(products_jsonl_path, bucket, BUCKET_PRODUCTS_DIR)

    # 3. Thực hiện export raw data 41m records lên GCS
    export_raw_data_to_jsonl(MICRO_BATCH_SIZE, RECORDS_PER_FILE)
    raw_jsonl_path = SUMMARY_JSONL_DIR
    process_raw_jsonl_to_gcs(raw_jsonl_path, bucket, BUCKET_RAW_DATA_DIR)

if __name__ == "__main__":
    export_to_gcs()