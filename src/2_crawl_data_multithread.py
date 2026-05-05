import requests
import json
import csv
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from bs4 import BeautifulSoup
import re
from fake_useragent import UserAgent
from dotenv import load_dotenv
from urllib.parse import quote_plus
import pymongo

# --- CẤU HÌNH ---
load_dotenv()

USERNAME = os.getenv("MONGO_USERNAME") or ""
PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD") or "")
HOST = os.getenv("MONGO_HOST") or "localhost"
PORT = os.getenv("MONGO_PORT") or "27017"
DB_NAME = os.getenv("MONGO_DB_NAME")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

# --- CẤU HÌNH ---
MAX_WORKERS = 20
MAX_RETRIES = 5
BATCH_SIZE = 50
OUTPUT_DIR = "output/crawl_data2"
LOG_DIR = "output/logs2"
RECORDS_PER_FILE = 1000

for d in [OUTPUT_DIR, LOG_DIR]: os.makedirs(d, exist_ok=True)

# Danh sách các thông tin bạn THỰC SỰ muốn lấy
KEYS_TO_KEEP = [
    "product_id",
    "name",
    "sku",
    "attribute_set_id",
    "attribute_set",
    "type_id",
    "price",
    "min_price",
    "max_price",
    "min_price_format",
    "max_price_format",
    "gold_weight",
    "none_metal_weight",
    "fixed_silver_weight",
    "material_design",
    "qty",
    "collection",
    "collection_id",
    "product_type",
    "product_type_value",
    "category",
    "category_name",
    "store_code",
    "platinum_palladium_info_in_alloy",
    "bracelet_without_chain",
    "show_popup_quantity_eternity",
    "gender"
]

# Khóa để bảo vệ file và biến đếm
file_lock = threading.Lock()
stats = {'total_success': 0}
ua = UserAgent()

# --- HÀM TRÍCH XUẤT ---
def extract_whitelist(data, keys_to_keep):
    """
    Chỉ giữ lại các key nằm trong danh sách cho trước và loại bỏ phần còn lại.
    """
    return {
        key: value for key, value in data.items() if key in keys_to_keep
    }

def extract_react_data(html_content):
    """
    Hàm dùng chung để trích xuất react_data từ bất kỳ nguồn HTML nào.
    """
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        # Tìm thẻ script có chứa chuỗi "react_data"
        script_tag = soup.find('script', string=re.compile(r'react_data\s*[:=]'))
        
        if not script_tag or not script_tag.string:
            return "MISSING_SCRIPT_TAG"

        # Dùng Regex để cắt lấy phần JSON
        # Pattern này tìm từ dấu { sau react_data cho đến khi gặp dấu }; ở cuối
        match = re.search(r'react_data\s*[:=]\s*({.+?});', script_tag.string, re.DOTALL)
        
        if match:
            json_str = match.group(1)
            data = json.loads(json_str)
            return data
        
        return "MISSING_JSON_MATCH"
    except json.JSONDecodeError:
        return "JSON_DECODE_ERROR"
    except Exception as e:
        return f"EXTRACTION_ERROR_{type(e).__name__}"
    
# --- GHI FILE AN TOÀN ---
def safe_write_jsonl(data):
    with file_lock:
        file_index = stats['total_success'] // RECORDS_PER_FILE
        file_path = os.path.join(OUTPUT_DIR, f"data_part_{file_index}.jsonl")
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")
        stats['total_success'] += 1

def safe_write_csv(file_name, row):
    file_path = os.path.join(LOG_DIR, file_name)
    file_exists = os.path.isfile(file_path)
    with file_lock:
        with open(file_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

# --- WORKER CHÍNH ---
def crawl_task(pid):
    url = f"https://www.glamira.vn/catalog/product/view/id/{pid}"
    try:
        # Giả lập trình duyệt để tránh 403
        headers = {'User-Agent': ua.random}
        resp = requests.get(url, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            html = resp.text
            data = extract_react_data(html)
            cleaned_data = extract_whitelist(data, KEYS_TO_KEEP) if isinstance(data, dict) else data
            
            if isinstance(cleaned_data, dict):
                record = {"product_id": pid, "data": cleaned_data, "crawled_at": datetime.now().isoformat()}
                return "SUCCESS", record

            return "ERR_NO_DATA", None
        return f"HTTP_{resp.status_code}", None
    except Exception as e:
        return f"ERR_{type(e).__name__}", None

def get_product_ids_from_db():
    """Hàm này có thể được sử dụng để lấy danh sách product_id trực tiếp từ MongoDB nếu cần."""
    try:
        if USERNAME:
            uri = f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/"
        else:
            uri = f"mongodb://{HOST}:{PORT}/"
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=10000)
        db = client[DB_NAME]
        products = list(db["cleaned_product_list"].find({}, {"_id": 0, "product_id": 1}))
        return [p['product_id'] for p in products]
    except Exception as e:
        print(f"✗ Lỗi khi kết nối hoặc truy vấn MongoDB: {e}")
        return []

# --- PIPELINE ---
def run_pipeline(product_ids):
    current_targets = list(product_ids)
    global_start = time.time()
    
    for attempt in range(1, MAX_RETRIES + 1):
        attempt_start = time.time()
        win_in_attempt = 0
        fail_in_attempt = 0
        next_retry_list = []
        
        print(f"\n>>> BẮT ĐẦU LẦT {attempt} ({len(current_targets)} SP)")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(crawl_task, pid) for pid in current_targets]
            
            for future in futures:
                status, result = future.result()
                
                if status == "SUCCESS":
                    win_in_attempt += 1
                    safe_write_jsonl(result)
                    pid = result["product_id"]
                    safe_write_csv("success.csv", {"id": pid, "attempt": attempt, "time": datetime.now()})
                else:
                    fail_in_attempt += 1
                    next_retry_list.append(pid)
                
                # Thống kê mỗi 50 sản phẩm
                total_processed = win_in_attempt + fail_in_attempt
                if total_processed % BATCH_SIZE == 0:
                    print(f"   [Thống kê] Đã xong {total_processed} SP | Thành công: {win_in_attempt} | Thất bại: {fail_in_attempt}")

        print(f"--- Kết quả lượt {attempt}: Thành công {win_in_attempt}, Thất bại {fail_in_attempt}")
        print(f"--- Thời gian lượt: {time.time() - attempt_start:.2f}s")
        
        current_targets = next_retry_list
        if not current_targets: break # Dừng nếu không còn sản phẩm lỗi

    # Sau 5 lần, ghi những sản phẩm thực sự thất bại
    for pid in current_targets:
        safe_write_csv("failed_final.csv", {"id": pid, "status": "FINAL_FAIL", "time": datetime.now()})

    print(f"\nTOTAL TIME: {time.time() - global_start:.2f}s | Final Success: {stats['total_success']}")

if __name__ == "__main__":
    product_ids = get_product_ids_from_db()
    run_pipeline(product_ids)